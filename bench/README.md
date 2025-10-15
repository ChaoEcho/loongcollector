基准测试（Kafka 输出插件）

概述

- 目标：在相同 Kafka 配置下，对三种输出进行基础能力（固定 Topic、可配置分区、不用动态 Topic/分区路由）的性能对比：
  1) loongcollector 原生 C++ 插件：`flusher_kafka_native`
  2) loongcollector Go 插件：`flusher_kafka_v2`
  3) Fluent Bit Kafka 输出

组成

- `compose.kafka.yaml`：单节点 Kafka + Zookeeper（暴露 `localhost:9092`）
- `compose.loong-native.yaml`：loongcollector + 原生 Kafka 插件（挂载本地日志文件与性能调优配置）
- `compose.loong-kafkav2.yaml`：loongcollector + Go Kafka 插件（挂载本地日志文件与性能调优配置）
- `compose.fluentbit.yaml`：Fluent Bit 挂载本地日志文件并输出到 Kafka
- `configs/loongcollector_config.json`：统一关闭 `max_bytes_per_sec` 限流并提升队列深度
- `configs/loong_native.yaml`：loongcollector 使用 `flusher_kafka_native` 的最小配置
- `configs/loong_kafkav2.yaml`：loongcollector 使用 `flusher_kafka_v2` 的最小配置
- `configs/fluent-bit.conf`：Fluent Bit 最小配置（Tail + Kafka 输出）
- `bench.py`：测试编排脚本，统一启动/停止组件、产生日志、消费统计并输出结果
- `generate_log.py`：日志生成脚本（可配置单行大小、速率）

先决条件

- 已安装 Docker 与 Docker Compose（WSL 下运行）
- Python 3.10+，本目录使用 `uv` 管理依赖
- 需要构建 loongcollector 开发镜像（一次性）供两种插件测试使用：
  - 在仓库根目录执行：`make e2edocker VERSION=0.0.1 DOCKER_REPOSITORY=aliyun/loongcollector`
  - 生成镜像：`aliyun/loongcollector:0.0.1`

安装依赖

- 在本目录执行：
  - `uv sync`

快速开始

1) 启动 Kafka：
   - `docker compose -f compose.kafka.yaml up -d`
2) 运行单项基准（示例：原生插件）：
   - Python 统计（便捷，吞吐较低时足够）：
     - `uv run python bench.py run --target native --duration 30 --rate-mb 25 --consumer python`
   - Kafka 自带 `kafka-consumer-perf-test` 统计（推荐，避免 Python 消费端瓶颈）：
     - `uv run python bench.py run --target native --duration 30 --rate-mb 25 --consumer perf`
3) 运行全部：
   - `uv run python bench.py run --target all --duration 30 --rate-mb 25 --consumer perf`

说明

- 所有测试均使用固定 Topic；分区数可通过 `--partitions` 配置（默认 6，仅允许增加分区）。
- 脚本会：
  1) 在 `data/<target>/input.log` 按指定速率产生日志（测试后自动清理）
  2) 启动目标容器 tail 该文件并写入固定 Topic `bench-basic`
  3) 从 `localhost:9092` 消费该 Topic，统计 N 秒内吞吐
  4) 停止容器、删除日志文件，并打印/保存结果

提示：终端里日志生成器在中断时打印的“最终平均速率”存在计算偏差（可能显示异常的超大值），以逐秒打印和消费端统计为准。

结果指标

- `messages`：消费到的消息总数
- `elapsed_sec`：统计窗口秒数
- `msg_per_sec`：平均消息吞吐（条/秒）
- `approx_mb_per_sec`：按消息平均字节估算的吞吐（MB/秒）
- 每次执行会生成 `results/benchmark-<UTC时间>.json` 和对应 Markdown 报告 `results/benchmark-<UTC时间>.md`

统一参数

- Kafka topic、分区与集群地址完全相同
- `acks=0`、`request.timeout.ms=30000`、`message.timeout.ms=300000`
- `retry.backoff.ms=100ms`、重试次数均为 10
- 缓冲与批量行为（三者已对齐）：
  - `queue.buffering.max.ms=100ms`（或等价的 `linger.ms=100ms`）
  - `batch.num.messages/BulkMaxSize=50000`
  - `queue.buffering.max.messages=1000000`
  - `queue.buffering.max.kbytes=1048576`
- `MaxMessageBytes=10485760`、压缩统一为 `none`
- loongcollector 全局关闭发送限速（`max_bytes_per_sec≈1GB/s`），并将 `DefaultLogQueueSize` 提升到 100000

常见问题

- loongcollector 镜像不存在：请先按“先决条件”构建开发镜像。
- Fluent Bit 镜像拉取失败：请确认 Docker 可访问公网或替换镜像源。
- WSL 下文件挂载权限：脚本会自动 `chmod /data`，如仍失败可手动执行 `chmod -R 777 data`（仅本地测试）。
- Fluent Bit 容器里缺少 `chmod` 命令：脚本会自动忽略该错误，不影响测试。
- 25MB/s 写入但消费端仅 2–3MB/s：请使用 `--consumer perf`，避免 Python 消费端成为瓶颈；如仍不足，可提高 Topic 分区数 `--partitions`。

可选参数

- `--files N`：并发写入 N 个日志文件（默认 1），用于模拟多文件 tail 并提升端到端并行度（公平性要求只约束 Kafka 客户端参数，此项按需使用）。
- `--line-bytes M`：控制生成单行的目标长度（含换行），便于评估“条数与单条大小”对吞吐的影响（默认使用内置行样例）。

单位与口径说明

- 文档与报告中的“MiB/s”都是二进制 MiB（1MiB=1024×1024），与十进制 MB（1MB=1,000,000）不同；40MiB/s ≈ 41.94MB/s。为避免歧义，基准的两种速率均以 MiB/s 表示：
  - Kafka MiB/s：消费者侧统计的消息负载字节速率（Kafka payload）。
  - Source MiB/s：文件系统侧真实写入速率（按窗口内 input-*.log 字节差/耗时）。

说明：Kafka payload 通常较 Source 略大（10%～30%），因日志会包装为 JSON 并携带元数据。
