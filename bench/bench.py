import argparse
import json
import os
import signal
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from kafka import KafkaConsumer
from rich.console import Console
from rich.table import Table


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
RESULTS_DIR = ROOT / "results"
CONFIG_DIR = ROOT / "configs"

console = Console()


def run_cmd(cmd, cwd=None, check=True):
    console.log(f"$ {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=cwd, check=check)


def compose_up(files):
    cmd = ["docker", "compose"]
    for f in files:
        cmd += ["-f", str(f)]
    cmd += ["up", "-d", "--remove-orphans"]
    run_cmd(cmd, cwd=ROOT)


def compose_down(files, services=None, remove_volumes=False):
    cmd = ["docker", "compose"]
    for f in files:
        cmd += ["-f", str(f)]
    if services:
        run_cmd(cmd + ["stop", *services], cwd=ROOT)
        run_cmd(cmd + ["rm", "-f", *services], cwd=ROOT)
        if remove_volumes:
            # prune bind-mounted data directory by caller separately
            pass
    else:
        down_cmd = ["down"]
        if remove_volumes:
            down_cmd.append("-v")
        run_cmd(cmd + down_cmd, cwd=ROOT)

def start_generator(rate_mb: float, outfile: Path) -> subprocess.Popen:
    ensure_dirs(outfile.parent)
    # recreate file to avoid读取旧内容
    if outfile.exists():
        outfile.unlink()
    outfile.touch()
    cmd = [sys.executable, str(ROOT / "generate_log.py"), "--rate", str(rate_mb), "--file", str(outfile)]
    proc = subprocess.Popen(cmd, cwd=ROOT)
    return proc


def stop_generator(proc: subprocess.Popen):
    if proc.poll() is None:
        try:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        except Exception:
            proc.kill()


def consume_metrics(topic: str, duration: int) -> dict:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="latest",
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
        group_id=f"bench-consumer-{int(time.time())}",
        max_poll_records=1000,
    )
    start = time.time()
    deadline = start + duration
    msg_count = 0
    total_bytes = 0
    try:
        while time.time() < deadline:
            for msg in consumer.poll(timeout_ms=500, max_records=1000).values():
                for record in msg:
                    msg_count += 1
                    if record.value:
                        total_bytes += len(record.value)
            time.sleep(0.05)
    finally:
        consumer.close()
    elapsed = max(0.001, time.time() - start)
    approx_mb = total_bytes / (1024 * 1024)
    return {
        "messages": msg_count,
        "elapsed_sec": round(elapsed, 2),
        "msg_per_sec": round(msg_count / elapsed, 2),
        "approx_mb_per_sec": round(approx_mb / elapsed, 2),
    }


def cleanup_data_dir(path: Path):
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)


def ensure_dirs(path: Path):
    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path, 0o777)


def ensure_container_data_permissions(compose_files, service: str):
    cmd = ["docker", "compose"]
    for f in compose_files:
        cmd += ["-f", str(f)]
    try:
        run_cmd(cmd + ["exec", "-T", service, "chmod", "777", "/data"], cwd=ROOT)
    except subprocess.CalledProcessError as exc:
        console.log(f"[yellow]跳过 chmod /data：{service} {exc}")


def run_target(target: str, rate_mb: float, duration: int) -> dict:
    # common compose base includes kafka
    base = [ROOT / "compose.kafka.yaml"]
    if target == "native":
        compose = base + [ROOT / "compose.loong-native.yaml"]
        topic = "bench-basic"
        data_dir = DATA_DIR / "native"
        service = "loong-native"
    elif target == "kafkav2":
        compose = base + [ROOT / "compose.loong-kafkav2.yaml"]
        topic = "bench-basic"
        data_dir = DATA_DIR / "kafkav2"
        service = "loong-kafkav2"
    elif target == "fluentbit":
        compose = base + [ROOT / "compose.fluentbit.yaml"]
        topic = "bench-basic"
        data_dir = DATA_DIR / "fluentbit"
        service = "fluent-bit"
    else:
        raise ValueError(f"unknown target: {target}")

    ensure_dirs(data_dir)
    # start compose services
    compose_up(compose)
    # allow services to settle
    time.sleep(5)
    ensure_container_data_permissions(compose, service)

    # start log generator and consume
    gen_proc = start_generator(rate_mb, data_dir / "input.log")
    # small warm-up
    time.sleep(2)
    metrics = {}
    error = None
    try:
        metrics = consume_metrics(topic, duration)
    except Exception as exc:  # noqa: BLE001
        error = exc
        raise
    finally:
        stop_generator(gen_proc)
        try:
            compose_down(compose, services=[service])
        finally:
            cleanup_data_dir(data_dir)
        if error:
            console.log(f"[red]Target {target} failed: {error}")

    return metrics


def main():
    parser = argparse.ArgumentParser(description="Kafka output benchmark runner")
    sub = parser.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="run benchmark")
    run.add_argument("--target", choices=["native", "kafkav2", "fluentbit", "all"], required=True)
    run.add_argument("--duration", type=int, default=30)
    run.add_argument("--rate-mb", type=float, default=50.0)

    args = parser.parse_args()

    if args.cmd == "run":
        # ensure kafka up
        compose_up([ROOT / "compose.kafka.yaml"])
        results = []
        targets = [args.target] if args.target != "all" else ["native", "kafkav2", "fluentbit"]
        for t in targets:
            console.rule(f"[bold]Running: {t}")
            try:
                m = run_target(t, args.rate_mb, args.duration)
                m["target"] = t
                results.append(m)
                console.log(f"{t} => {m}")
            except subprocess.CalledProcessError as e:
                console.log(f"[red]Command failed for {t}: {e}")
            except Exception as e:
                console.log(f"[red]Error for {t}: {e}")

        # pretty print
        table = Table(title="Kafka Output Benchmark")
        table.add_column("Target")
        table.add_column("Msgs")
        table.add_column("Secs")
        table.add_column("Msgs/s")
        table.add_column("MB/s (approx)")
        for m in results:
            table.add_row(
                m.get("target", "-"),
                str(m.get("messages", 0)),
                str(m.get("elapsed_sec", 0)),
                str(m.get("msg_per_sec", 0)),
                str(m.get("approx_mb_per_sec", 0)),
            )
        console.print(table)

        RESULTS_DIR.mkdir(exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        result_file = RESULTS_DIR / f"benchmark-{timestamp}.json"
        payload = {
            "timestamp_utc": timestamp,
            "params": {
                "duration": args.duration,
                "rate_mb": args.rate_mb,
                "target": args.target,
            },
            "results": results,
        }
        result_file.write_text(json.dumps(payload, indent=2))
        console.log(f"结果已保存: {result_file.relative_to(ROOT)}")

        # 额外生成 Markdown 对比报告，便于可视化对比
        markdown_file = RESULTS_DIR / f"benchmark-{timestamp}.md"
        max_msgs = max((m.get("msg_per_sec", 0) or 0) for m in results) if results else 0
        bar_width = 30
        lines = ["# Kafka 输出基准对比", "", f"- UTC 时间：{timestamp}", f"- 测试目标：{args.target}", f"- 时长：{args.duration}s", f"- 日志生成速率：{args.rate_mb} MB/s", ""]
        lines.append("| Target | Msgs | Msgs/s | MB/s | 可视化 |")
        lines.append("| --- | ---: | ---: | ---: | --- |")
        for m in results:
            msgs = m.get("messages", 0)
            msg_rate = m.get("msg_per_sec", 0)
            mb_rate = m.get("approx_mb_per_sec", 0)
            if max_msgs > 0 and msg_rate > 0:
                ratio = min(1.0, msg_rate / max_msgs)
                bar_len = max(1, int(ratio * bar_width))
            else:
                bar_len = 1
            bar = "=" * bar_len if msg_rate > 0 else "."
            lines.append(f"| {m.get('target', '-') } | {msgs:,} | {msg_rate:,.2f} | {mb_rate:,.2f} | {bar} |")
        markdown_file.write_text("\n".join(lines))
        console.log(f"Markdown 对比: {markdown_file.relative_to(ROOT)}")

        # 所有目标跑完后默认关闭 Kafka 栈（保留数据卷）
        compose_down([ROOT / "compose.kafka.yaml"])


if __name__ == "__main__":
    main()
