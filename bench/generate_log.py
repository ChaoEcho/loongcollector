import time
import argparse


def main():
    parser = argparse.ArgumentParser(
        description="Write repeated lines to a file at a specified rate."
    )
    parser.add_argument("--rate", type=float, required=True, help="写入速率（MB/秒）")
    parser.add_argument("--file", type=str, required=True, help="输出文件路径")
    parser.add_argument(
        "--line-bytes",
        type=int,
        default=0,
        help="每行字节数（含换行，0 表示使用默认样例行长度）",
    )
    args = parser.parse_args()

    base_line = '203.0.113.45 - - [25/Jun/2024:23:59:59 +0000] "GET /wp-admin/admin-ajax.php?action=revslider_ajax_action&client_action=get_facebook HTTP/1.1" 200 1847 "https://www.google.com/search?q=free+piano+sheet+music+pdf+download+site%3Aexample.com&ref=lnms&sa=X&biw=1920&bih=1080" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)" rt=0.312 uct="0.001" uht="0.125" urt="0.311" sid=a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0 uagent_hash=7d8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7 request_id=req-20240625-235959-001'
    # 调整行长度（含换行）以降低消息条数，提高每条大小
    if args.line_bytes and args.line_bytes > 0:
        need = max(0, args.line_bytes - 1 - len(base_line.encode("utf-8")))
        if need > 0:
            base_line = base_line + " X" * (need // 2)
        line_content = base_line + "\n"
    else:
        line_content = base_line + "\n"
    bytes_per_line = len(line_content.encode("utf-8"))
    target_rate = args.rate

    chunk_size_bytes = 1 * 1024 * 1024
    lines_per_chunk = chunk_size_bytes // bytes_per_line

    chunk_data = (line_content * lines_per_chunk).encode("utf-8")
    actual_chunk_size = len(chunk_data)

    if actual_chunk_size == 0:
        print("错误：单行内容过长，无法形成有效数据块")
        return

    last_print_time = time.time()
    accumulated_bytes = 0
    total_written = 0

    time_per_chunk = actual_chunk_size / (target_rate * 1024 * 1024)

    try:
        with open(args.file, "ab") as f:
            print(f"开始以 {target_rate} MB/秒 的速度写入文件...")
            while True:
                start_time = time.time()
                f.write(chunk_data)
                f.flush()

                current_time = time.time()
                accumulated_bytes += actual_chunk_size
                total_written += actual_chunk_size

                if current_time - last_print_time >= 1:
                    elapsed = current_time - last_print_time
                    actual_rate = accumulated_bytes / elapsed / (1024 * 1024)
                    print(
                        f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] "
                        f"当前速率：{actual_rate:.2f} MB/s | "
                        f"累计写入：{total_written/(1024*1024):.2f} MB | "
                        f"目标速率：{target_rate} MB/s"
                    )
                    accumulated_bytes = 0
                    last_print_time = current_time

                elapsed = time.time() - start_time
                sleep_duration = time_per_chunk - elapsed
                if sleep_duration > 0:
                    time.sleep(sleep_duration)

    except KeyboardInterrupt:
        final_rate = (
            total_written / (time.time() - (last_print_time - elapsed)) / (1024 * 1024)
        )
        print(f"\n写入已停止，最终平均速率：{final_rate:.2f} MB/s")


if __name__ == "__main__":
    main()
