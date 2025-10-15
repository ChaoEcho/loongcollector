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

try:
    # 优先使用高性能的 librdkafka Python 绑定
    from confluent_kafka import Consumer as CConsumer, TopicPartition
    _HAS_CONFLUENT = True
except Exception:  # noqa: BLE001
    _HAS_CONFLUENT = False
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


def run_cmd_out(cmd, cwd=None, check=True) -> subprocess.CompletedProcess:
    console.log(f"$ {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=cwd, check=check, capture_output=True, text=True)


def _sum_files_bytes(dir_path: Path, pattern: str = "*.log") -> int:
    total = 0
    try:
        for p in dir_path.glob(pattern):
            try:
                total += p.stat().st_size
            except FileNotFoundError:
                # file may rotate/delete in between
                continue
    except Exception:
        pass
    return total


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


def ensure_topic_partitions(
    compose_files, topic: str, partitions: int, replication_factor: int = 1
):
    """Ensure the Kafka topic exists with at least the given partition count.
    - create if not exists with desired partitions
    - if exists but partitions < desired, alter to increase partitions (Kafka only allows increasing)
    """
    if partitions is None or partitions <= 0:
        return
    cmd = ["docker", "compose"]
    for f in compose_files:
        cmd += ["-f", str(f)]

    # Wait for kafka to be ready to accept admin requests
    ready = False
    for _ in range(30):
        try:
            proc = run_cmd_out(
                cmd
                + [
                    "exec",
                    "-T",
                    "kafka",
                    "kafka-topics",
                    "--bootstrap-server",
                    "kafka:29092",
                    "--list",
                ],
                cwd=ROOT,
                check=False,
            )
            if proc.returncode == 0:
                ready = True
                break
        except Exception:
            pass
        time.sleep(1)
    if not ready:
        console.log("[yellow]Kafka 可能尚未完全就绪，尝试继续创建/描述 topic……")

    # Create if not exists
    try:
        run_cmd(
            cmd
            + [
                "exec",
                "-T",
                "kafka",
                "kafka-topics",
                "--bootstrap-server",
                "kafka:29092",
                "--create",
                "--if-not-exists",
                "--topic",
                topic,
                "--partitions",
                str(partitions),
                "--replication-factor",
                str(replication_factor),
            ],
            cwd=ROOT,
        )
    except subprocess.CalledProcessError as e:
        console.log(f"[yellow]Topic create returned error (may already exist): {e}")

    # Describe to get current partition count
    try:
        proc = run_cmd_out(
            cmd
            + [
                "exec",
                "-T",
                "kafka",
                "kafka-topics",
                "--bootstrap-server",
                "kafka:29092",
                "--describe",
                "--topic",
                topic,
            ],
            cwd=ROOT,
        )
        desc = proc.stdout or ""
        current = None
        # Try to parse PartitionCount from the summary line
        for line in desc.splitlines():
            if "PartitionCount:" in line:
                try:
                    # e.g., "Topic: bench-basic\tTopicId: ...\tPartitionCount: 1\tReplicationFactor: 1 ..."
                    parts = line.replace("\t", " ").split()
                    for i, tok in enumerate(parts):
                        if tok.startswith("PartitionCount:"):
                            val = tok.split(":", 1)[1]
                            current = int(val)
                            break
                except Exception:
                    pass
                break
        # Fallback: count Partition lines
        if current is None:
            cnt = 0
            for line in desc.splitlines():
                if "Partition:" in line and "Topic:" in line:
                    cnt += 1
            if cnt > 0:
                current = cnt

        if current is None:
            console.log("[yellow]未能解析当前分区数，跳过 alter")
            return

        if current < partitions:
            console.log(f"[cyan]Increase partitions: {current} -> {partitions}")
            run_cmd(
                cmd
                + [
                    "exec",
                    "-T",
                    "kafka",
                    "kafka-topics",
                    "--bootstrap-server",
                    "kafka:29092",
                    "--alter",
                    "--topic",
                    topic,
                    "--partitions",
                    str(partitions),
                ],
                cwd=ROOT,
            )
    except subprocess.CalledProcessError as e:
        console.log(f"[red]ensure_topic_partitions failed: {e}")


def start_generator(rate_mb: float, outfile: Path, line_bytes: int | None = None) -> subprocess.Popen:
    ensure_dirs(outfile.parent)
    # recreate file to avoid读取旧内容
    if outfile.exists():
        outfile.unlink()
    outfile.touch()
    cmd = [
        sys.executable,
        str(ROOT / "generate_log.py"),
        "--rate",
        str(rate_mb),
        "--file",
        str(outfile),
    ]
    if line_bytes and line_bytes > 0:
        cmd += ["--line-bytes", str(line_bytes)]
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


def consume_metrics_python(topic: str, duration: int) -> dict:
    """消费统计吞吐。

    - 优先使用 confluent-kafka（librdkafka）以避免 Python 端成为瓶颈。
    - 如不可用则回退到 kafka-python。
    """
    if _HAS_CONFLUENT:
        conf = {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"bench-consumer-{int(time.time())}",
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
            # 拉取批量与缓冲调优，降低 Python 交互开销
            "fetch.min.bytes": 5 * 1024 * 1024,
            "fetch.wait.max.ms": 50,
            "queued.max.messages.kbytes": 1024 * 1024,  # 1GB 队列
            "max.partition.fetch.bytes": 32 * 1024 * 1024,
            "queued.min.messages": 100000,
        }
        c = CConsumer(conf)
        c.subscribe([topic])
        start = time.time()
        deadline = start + duration
        msg_count = 0
        total_bytes = 0
        try:
            while time.time() < deadline:
                # 批量拉取，显著降低 Python 循环成本
                msgs = c.consume(num_messages=50000, timeout=0.05)
                if not msgs:
                    continue
                msg_count += len(msgs)
                for m in msgs:
                    if m is None or m.error():
                        continue
                    v = m.value()
                    if v is not None:
                        total_bytes += len(v)
        finally:
            c.close()
        elapsed = max(0.001, time.time() - start)
        approx_mb = total_bytes / (1024 * 1024)
        return {
            "messages": msg_count,
            "elapsed_sec": round(elapsed, 2),
            "msg_per_sec": round(msg_count / elapsed, 2),
            "approx_mb_per_sec": round(approx_mb / elapsed, 2),
        }

    # 回退：kafka-python（性能较差，仅用于兜底）
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="latest",
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
        group_id=f"bench-consumer-{int(time.time())}",
        max_poll_records=1000,
        fetch_max_bytes=32 * 1024 * 1024,
        max_partition_fetch_bytes=32 * 1024 * 1024,
        fetch_min_bytes=5 * 1024 * 1024,
    )
    start = time.time()
    deadline = start + duration
    msg_count = 0
    total_bytes = 0
    try:
        while time.time() < deadline:
            batch = consumer.poll(timeout_ms=200, max_records=10000)
            for msgs in batch.values():
                for record in msgs:
                    msg_count += 1
                    if record.value:
                        total_bytes += len(record.value)
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


def consume_metrics_perf(topic: str, duration: int) -> dict:
    """借助 Kafka 自带的 kafka-consumer-perf-test 在容器内测量吞吐。

    使用 timeout 约束时长，解析最终统计。该工具以 MB/s 和 records/s 输出，避免 Python 循环开销。
    """
    cmd = [
        "docker",
        "compose",
        "-f",
        str(ROOT / "compose.kafka.yaml"),
        "exec",
        "-T",
        "kafka",
        "bash",
        "-lc",
        # 以秒为单位控制时长（留 2~3 秒余量以便输出汇总），设置极大 messages 以避免过早退出
        f"timeout {duration+3}s kafka-consumer-perf-test --bootstrap-server kafka:29092 --topic {topic} --from-latest --messages 100000000 --reporting-interval 1000 --timeout 30000 --print-metrics --show-detailed-stats",
    ]
    proc = run_cmd_out(cmd, cwd=ROOT, check=False)
    out = proc.stdout or ""
    messages = 0
    mbps = 0.0
    rps = 0.0
    mbps_list = []
    rps_list = []
    # 解析汇总行，兼容不同版本输出
    import re
    line_re = re.compile(
        r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}:\d{3},\s*\d+,\s*([0-9.]+),\s*([0-9.]+),\s*([0-9]+),\s*([0-9.]+)"
    )
    for line in out.splitlines():
        line = line.strip()
        m = line_re.match(line)
        if m:
            try:
                mbps_list.append(float(m.group(2)))
                rps_list.append(float(m.group(4)))
            except Exception:
                pass
        if line.lower().startswith("total records consumed"):
            try:
                # total records consumed: 123456
                messages = int(line.split(":", 1)[1].strip())
            except Exception:
                pass
    # 计算平均
    if mbps_list:
        mbps = sum(mbps_list) / len(mbps_list)
    if rps_list:
        rps = sum(rps_list) / len(rps_list)
    return {
        "messages": messages,
        "elapsed_sec": duration,
        "msg_per_sec": round(rps, 2),
        "approx_mb_per_sec": round(mbps, 2),
        "raw": out[-2000:],
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


def run_target(
    target: str, rate_mb: float, duration: int, files: int, line_bytes: int | None, consumer: str
) -> dict:
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

    # start log generator(s) and consume
    gens = []
    files = max(1, int(files))
    per_file_rate = max(0.1, rate_mb / files)
    for i in range(files):
        gens.append(start_generator(per_file_rate, data_dir / f"input-{i}.log", line_bytes=line_bytes))
    # small warm-up
    time.sleep(2)
    # record source bytes and time before consume window
    pre_time = time.time()
    pre_bytes = _sum_files_bytes(data_dir)
    metrics = {}
    error = None
    try:
        if consumer == "perf":
            metrics = consume_metrics_perf(topic, duration)
        else:
            metrics = consume_metrics_python(topic, duration)
    except Exception as exc:  # noqa: BLE001
        error = exc
        raise
    finally:
        # record source bytes and time after consume window
        post_time = time.time()
        post_bytes = _sum_files_bytes(data_dir)
        for p in gens:
            stop_generator(p)
        try:
            compose_down(compose, services=[service])
        finally:
            cleanup_data_dir(data_dir)
        if error:
            console.log(f"[red]Target {target} failed: {error}")
    # dual-metrics: source vs kafka payload
    try:
        delta_sec = max(0.001, (post_time - pre_time))
        source_mb_per_sec = max(0.0, (post_bytes - pre_bytes) / (1024 * 1024) / delta_sec)
    except Exception:
        source_mb_per_sec = 0.0
    # rename/add fields for clarity
    kafka_payload_mb_per_sec = float(metrics.get("approx_mb_per_sec", 0.0))
    metrics["kafka_payload_mb_per_sec"] = round(kafka_payload_mb_per_sec, 2)
    metrics["source_mb_per_sec"] = round(source_mb_per_sec, 2)
    return metrics


def main():
    parser = argparse.ArgumentParser(description="Kafka output benchmark runner")
    sub = parser.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="run benchmark")
    run.add_argument(
        "--target", choices=["native", "kafkav2", "fluentbit", "all"], required=True
    )
    run.add_argument("--duration", type=int, default=30)
    run.add_argument("--rate-mb", type=float, default=50.0)
    run.add_argument("--files", type=int, default=1, help="并发写入的日志文件个数（默认 1）")
    run.add_argument("--line-bytes", type=int, default=0, help="每条日志的目标字节数（含换行），默认 0 表示使用内置样例长度")
    run.add_argument("--consumer", choices=["python", "perf"], default="perf", help="吞吐统计方式：python 或 kafka-consumer-perf-test(默认)")
    run.add_argument(
        "--partitions", type=int, default=6, help="Kafka topic 分区数（默认 6）"
    )

    args = parser.parse_args()

    if args.cmd == "run":
        # ensure kafka up
        compose_up([ROOT / "compose.kafka.yaml"])
        # ensure topic partitions before any producers start
        ensure_topic_partitions(
            [ROOT / "compose.kafka.yaml"],
            topic="bench-basic",
            partitions=args.partitions,
        )
        results = []
        targets = (
            [args.target]
            if args.target != "all"
            else ["native", "kafkav2", "fluentbit"]
        )
        for t in targets:
            console.rule(f"[bold]Running: {t}")
            try:
                m = run_target(t, args.rate_mb, args.duration, args.files, args.line_bytes, args.consumer)
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
        table.add_column("Kafka MiB/s")
        table.add_column("Source MiB/s")
        for m in results:
            table.add_row(
                m.get("target", "-"),
                str(m.get("messages", 0)),
                str(m.get("elapsed_sec", 0)),
                str(m.get("msg_per_sec", 0)),
                str(m.get("kafka_payload_mb_per_sec", m.get("approx_mb_per_sec", 0))),
                str(m.get("source_mb_per_sec", 0)),
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
                "partitions": args.partitions,
            },
            "results": results,
        }
        result_file.write_text(json.dumps(payload, indent=2))
        console.log(f"结果已保存: {result_file.relative_to(ROOT)}")

        # 额外生成 Markdown 对比报告，便于可视化对比
        markdown_file = RESULTS_DIR / f"benchmark-{timestamp}.md"
        max_msgs = (
            max((m.get("msg_per_sec", 0) or 0) for m in results) if results else 0
        )
        bar_width = 30
        lines = [
            "# Kafka 输出基准对比",
            "",
            f"- UTC 时间：{timestamp}",
            f"- 测试目标：{args.target}",
            f"- 时长：{args.duration}s",
            f"- 日志生成速率：{args.rate_mb} MB/s",
            f"- 分区数：{args.partitions}",
            "",
        ]
        lines.append("| Target | Msgs | Msgs/s | Kafka MiB/s | Source MiB/s | 可视化 |")
        lines.append("| --- | ---: | ---: | ---: | ---: | --- |")
        for m in results:
            msgs = m.get("messages", 0)
            msg_rate = m.get("msg_per_sec", 0)
            mb_rate = m.get("kafka_payload_mb_per_sec", m.get("approx_mb_per_sec", 0))
            src_rate = m.get("source_mb_per_sec", 0)
            if max_msgs > 0 and msg_rate > 0:
                ratio = min(1.0, msg_rate / max_msgs)
                bar_len = max(1, int(ratio * bar_width))
            else:
                bar_len = 1
            bar = "=" * bar_len if msg_rate > 0 else "."
            lines.append(
                f"| {m.get('target', '-') } | {msgs:,} | {msg_rate:,.2f} | {mb_rate:,.2f} | {src_rate:,.2f} | {bar} |"
            )
        markdown_file.write_text("\n".join(lines))
        console.log(f"Markdown 对比: {markdown_file.relative_to(ROOT)}")

        # 所有目标跑完后默认关闭 Kafka 栈（保留数据卷）
        compose_down([ROOT / "compose.kafka.yaml"])


if __name__ == "__main__":
    main()
