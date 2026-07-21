# RayDispatcher

`ray_dispatcher` 包提供三个单次轮询函数及对应的后台循环：

- `data_listener()`：读取 Kafka 分区 high watermark，或 Postgres 稳定复合游标和窗口 count。
- `ray_trigger()`：结合 backlog、Handler/输出端并发限制、全局 in-flight 限制和当前可用 CPU，生成 Ray task/Actor 调用。
- `ray_status()`：非阻塞轮询 ObjectRef，失败重试；永久失败写入 FailureStore 后跳过区间并推进 checkpoint。
- `start()` / `stop()`：定时并发运行上述三个循环。

包布局：

| 模块 | 职责 |
|---|---|
| [`ray_dispatcher/dispatcher.py`](./ray_dispatcher/dispatcher.py) | 调度核心 |
| [`ray_dispatcher/discovery.py`](./ray_dispatcher/discovery.py) | workers 目录扫描 |
| [`ray_dispatcher/sources.py`](./ray_dispatcher/sources.py) | 固定 SourceObserver（水位/切分） |
| [`ray_dispatcher/backend.py`](./ray_dispatcher/backend.py) | Native Ray 执行后端 |
| [`ray_dispatcher/failures.py`](./ray_dispatcher/failures.py) | 永久失败区间落盘 |
| [`ray_dispatcher/policy.py`](./ray_dispatcher/policy.py) | 渐进式调度策略 |

调度核心没有强制第三方依赖；生产适配器按需加载。可用 `pip install -e .` 安装本包。

## 推荐方式：扫描 workers 目录

目录结构：

```text
app/
├── pyproject.toml
├── ray_dispatcher/
│   ├── __init__.py
│   ├── dispatcher.py
│   ├── discovery.py
│   ├── sources.py
│   └── ...
├── example_app/
│   ├── main.py
│   └── workers/
│       └── orders.py
└── tests/
```

`workers/events.py` 可以声明多个处理函数。下面两个 Handler 加入同一个共享来源组：Kafka 区间只由
`fetch_events` 读取一次，返回的 Ray ObjectRef 再传给两个函数：

```python
from my_pipeline import write_csv_range, write_jsonl_range


COMMON_SOURCE = {
    "kind": "kafka",
    "source_id": "events-v1",
    "connection_id": "events-kafka",
    "brokers": ["kafka-1:9092", "kafka-2:9092"],
    "topic": "events",
    "initial_offset": "latest",
}

HANDLERS = [
    {
        "name": "events_to_jsonl",
        "handler_id": "events:events_to_jsonl",
        "entrypoint": "events_to_jsonl",
        "sources": [COMMON_SOURCE],
        "output": {
            "connection_id": "archive-store",
            "target": "events/jsonl/",
            "output_format": "jsonl",
            "max_parallelism": 8,
        },
        "batch_size": 50_000,
        "max_parallelism": 8,
        "shared_source_group": "events-fanout",
        "data_fetcher": "fetch_events",
        "fetcher_id": "events-reader-v1",
    },
    {
        "name": "events_to_csv",
        "handler_id": "events:events_to_csv",
        "entrypoint": "events_to_csv",
        "sources": [COMMON_SOURCE],
        "output": {
            "connection_id": "report-store",
            "target": "events/csv/",
            "output_format": "csv",
            "max_parallelism": 3,
        },
        "batch_size": 10_000,
        "max_parallelism": 4,
        "shared_source_group": "events-fanout",
        "data_fetcher": "fetch_events",
        "fetcher_id": "events-reader-v1",
    },
]


def fetch_events(request):
    return read_kafka_range(
        request.topic,
        request.partition,
        request.start_offset,
        request.end_offset,
    )


def events_to_jsonl(request, records):
    return write_jsonl_range(request, records)


def events_to_csv(request, records):
    return write_csv_range(request, records)
```

`main.py` 不再手工传入 worker 或 topic：

```python
import asyncio
from pathlib import Path

import ray

from ray_dispatcher import RayDispatcher, SchedulingPolicy, SQLiteCheckpointStore, SQLiteFailureStore


async def main():
    ray.init()
    async with RayDispatcher(
        Path(__file__).parent / "workers",
        checkpoint_store=SQLiteCheckpointStore("dispatcher-checkpoints.sqlite3"),
        failure_store=SQLiteFailureStore("dispatcher-failures.sqlite3"),
        policy=SchedulingPolicy(max_in_flight=64),
    ) as dispatcher:
        await asyncio.Event().wait()


asyncio.run(main())
```

启动过程为：扫描所有 `workers/*.py` → 展开每个模块的 `HANDLERS` → 每个普通函数自动包装成
独立 Ray remote function → 汇总并去重所有 Kafka topic/Postgres table。`ray_backend` 为关键字参数且可省略；
水位观察固定为包内 `SourceObserver`。也可用 `async with dispatcher` 代替手动 `start()` / `stop()`。
之后每次 `data_listener()` 都会主动：

- 调用 Kafka `list_topics()` 发现分区；
- 对每个分区调用 `get_watermark_offsets(..., cached=False)` 获取实时 low/high offset；
- 和 Dispatcher 持有的 committed checkpoint 比较，得到 backlog；
- 对 Postgres 查询最大 `(timestamp_column, primary_key_column)`，并统计游标窗口内的 count。

这里的 offset 不是 worker 传进来的。worker 只负责声明自己关注哪些来源，实际 offset 由
`SourceObserver` 主动向 broker 查询，然后由 Dispatcher 切分并传给 worker。

生产数据源实现需要可选依赖：

```bash
pip install ray confluent-kafka asyncpg
```

完整可运行的双 Handler 示例见 [example_app](./example_app/README.md)：同一来源只获取一次数据，
通过 Ray ObjectRef 分别输出 JSONL 和 CSV，并包含共享 checkpoint、批量拆分和输出并发限制。

## 一次获取，多 Handler 处理

共享模式是显式 opt-in。组内每个 Handler 必须配置相同的：

```python
"shared_source_group": "events-fanout",
"data_fetcher": "fetch_events",
"fetcher_id": "events-reader-v1",
```

`data_fetcher(request)` 负责读取 Kafka/Postgres 区间并返回数据；组内 Handler 的默认签名为
`handler(request, records)`。Dispatcher 的执行顺序是：

```text
一个 offset slice
  -> 一个 fetch task
  -> 一个 Ray ObjectRef
  -> N 个 Handler task（ObjectRef 作为顶层第二参数）
  -> N 个 Handler 全部成功
  -> 推进共享 checkpoint
```

Handler 业务失败重试会复用原 fetch ObjectRef，不会再次读取 Kafka。fetch 自身失败则按
`fetch_max_retries` 重试；永久失败会先写入 `FailureStore`（区间元信息 + 可物化的 fetch payload），
再跳过该区间、推进 checkpoint，并解除来源阻塞。成功提交 checkpoint 后 Dispatcher 会释放本批
ObjectRef，避免历史数据长期占用 Object Store。

同组 Handler 必须声明同一个物理来源、`source_id`、`fetcher_id`、首次 offset 和 retention 策略；
当前实现还要求每个共享 Handler 只声明一个 source，且不使用 `args_builder`。未配置共享组时保留原来的
独立消费、独立 checkpoint 行为。

旧 Worker 模块仍可导出 `WORKER_CONFIG`/`WORKER_SPEC`，或者实现
`get_worker_spec() -> WorkerSpec`；它们会被兼容成单 Handler。以下手工构造方式适合动态生成 Handler。

## 手工构造方式

```python
import asyncio
import time
import ray
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from ray_dispatcher import (
    DispatchRequest,
    KafkaSource,
    NativeRayBackend,
    RayDispatcher,
    SchedulingPolicy,
    SQLiteCheckpointStore,
    SQLiteFailureStore,
    WorkerSpec,
)


def process_partition(topic, partition, start, end, idempotency_key):
    """处理一个 Kafka 分区的半开 offset 区间 [start, end)。"""
    consumer = Consumer({
        "bootstrap.servers": "kafka-1:9092,kafka-2:9092",
        "group.id": "ray-dispatcher-workers",
        "enable.auto.commit": False,
        "auto.offset.reset": "error",
        "enable.partition.eof": True,
    })
    cursor = TopicPartition(topic, partition, start)
    processed = 0
    last_progress = time.monotonic()
    try:
        # assign + 指定 offset，不依赖 consumer group 当前提交位置。
        consumer.assign([cursor])
        while True:
            message = consumer.poll(1.0)
            if message is None:
                if time.monotonic() - last_progress > 30:
                    raise TimeoutError(f"Kafka range [{start}, {end}) made no progress")
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    # EOF 消息的 offset 是该分区当前 high watermark。
                    if message.offset() >= end:
                        break
                    continue
                raise KafkaException(message.error())
            if message.offset() >= end:
                break

            # 这里才是业务逻辑：解析、计算并写入目标系统。
            # 单条数据的幂等键必须包含 partition + offset，不能只用批次 ID。
            event_key = f"{topic}:{partition}:{message.offset()}"
            persist_event(
                payload=message.value(),
                event_key=event_key,
                dispatch_id=idempotency_key,
            )
            processed += 1
            last_progress = time.monotonic()
    finally:
        consumer.close()

    return {
        "topic": topic,
        "partition": partition,
        "start_offset": start,
        "end_offset": end,
        "processed": processed,
    }


@ray.remote(max_retries=0)  # 重试由 Dispatcher 统一管理
def consume_events(request: DispatchRequest):
    # Kafka 必须只消费 [start_offset, end_offset)，不要在 worker 中自行提交 offset。
    # dispatch_id 用于批次追踪；单条消息再结合 partition + offset 做幂等。
    if request.topic is None:
        raise ValueError("Kafka DispatchRequest must contain topic")
    return process_partition(
        topic=request.topic,
        partition=request.partition,
        start=request.start_offset,
        end=request.end_offset,
        idempotency_key=request.dispatch_id,
    )


source = KafkaSource(
    source_id="events-v1",
    brokers=("kafka-1:9092", "kafka-2:9092"),
    topic="events",
    initial_offset="latest",  # 首次启动不处理历史；改成 earliest 可回放
)

worker = WorkerSpec(
    name="event-normalizer",
    worker=consume_events,
    sources=(source,),
    max_parallelism=8,
    batch_size=50_000,
    cpus_per_task=1,
)


async def main():
    ray.init()
    async with RayDispatcher(
        workers=(worker,),
        # ray_backend 可省略；默认 NativeRayBackend。水位观察固定为 SourceObserver
        checkpoint_store=SQLiteCheckpointStore("dispatcher-checkpoints.sqlite3"),
        failure_store=SQLiteFailureStore("dispatcher-failures.sqlite3"),
        policy=SchedulingPolicy(max_in_flight=64),
        operation_timeout=30,
    ) as dispatcher:
        await asyncio.Event().wait()


asyncio.run(main())
```

上面的 `process_partition()` 不是 `RayDispatcher` 内置函数，而是 worker 的业务执行函数。
Dispatcher 只负责决定“哪个分区、从哪里开始、到哪里结束”；它不知道消息如何解析、计算后写入哪个系统。
示例使用 `confluent-kafka` 完成精确区间消费，其中只有 `persist_event()` 需要按业务实现，例如写入
Postgres、ClickHouse、对象存储或调用后续计算。建议 `persist_event()` 通过
`topic + partition + offset` 建立唯一键，并把 `dispatch_id` 作为批次追踪字段。worker 不调用
`consumer.commit()`；所有 slice 成功后由 Dispatcher 推进自己的 checkpoint。

`DispatchRequest` 会显式携带来源路由信息：Kafka 请求包含 `topic`、`partition` 和
`source_connection_id`，Postgres 请求包含 `table` 和 `source_connection_id`。其中
`source_connection_id` 是外部连接配置的引用，不应把密码或活连接对象塞进请求。

如果已有 worker 函数签名是
`worker(startoffset, endoffset, n, task_index, partition, topic, dispatch_id)`，在
`WorkerSpec` 上设置 `args_builder=offset_kwargs_builder` 即可。

## SourceObserver（固定水位观察）

水位观察与 Postgres 区间规划由包内固定的 `SourceObserver` 完成，不是可插拔扩展点；业务 Handler /
`data_fetcher` 负责读写真实数据。`SourceObserver` 会：

1. 查询 Kafka 分区 low/high watermark；
2. 查询 Postgres 稳定上界游标，并统计窗口 count；
3. 在数据库内按 `(timestamp, primary_key)` 排序，为最多 `n × batch_size` 行生成有序且不重叠的
   复合游标范围（每个 task 处理自己的 `(start, end]`）。

Postgres 推荐查询形式（表名和列名只能来自受信配置，不能作为 SQL 参数）：

```sql
SELECT updated_at, id
FROM orders
ORDER BY updated_at DESC, id DESC
LIMIT 1;

SELECT count(*)
FROM orders
WHERE (updated_at, id) > ($1, $2)
  AND (updated_at, id) <= ($3, $4);
```

单纯比较两次全表 count 会被 update/delete 抵消，因此实现没有采用该方式。

## Task 还是 Actor

- 默认用 Task：区间相互独立、无状态、易扩缩。
- 用 Actor：需要缓存模型/历史数据、复用连接、维持分区有序状态。配置
  `mode=ExecutionMode.ACTOR, remote_method="process"` 后，每个 Handler 只创建并复用
  **一个** Actor；`max_parallelism` 限制该 Actor 上的 in-flight method 数。
- 影响正确性的历史状态不能只放 Actor 内存；Actor 重启后必须能从外部 checkpoint/snapshot 恢复。

## 状态、失败与一致性

所有运行状态都保存在 `dispatcher.state`：

- `state.sources`：committed/observed cursor、backlog、到达和处理速率。
- `state.batches`：来源窗口及整批状态。
- `state.runs`：每个 task 的状态、attempt、结果和错误。
- `state.refs`：当前进程已记录的 Ray ref。
- `dispatcher.snapshot()`：适合日志或监控输出的无敏感连接信息快照。

构造时 `ray_backend` 可省略（默认 `NativeRayBackend`）；水位观察固定使用 `SourceObserver`。
失败落盘默认 `MemoryFailureStore`，生产可用 `SQLiteFailureStore`。测试/demo 可通过赋值
`dispatcher.source_observer` 替换观察器（鸭子类型），不作为正式扩展 API。

调度排序为渐进字典序：静态 priority → backlog 最多 → 最慢（积压耗时）→ 等待最久；
然后再检查 slot / CPU / 输出并发。checkpoint 在一批全部成功时推进；永久失败时先写入
`FailureStore`（区间、错误摘要；共享模式还会尽量物化 fetch payload），再推进到
`batch.end`（跳过毒区间）并清除 `active_batch_id`，来源可继续往后调度。独立 Handler 模式
Driver 侧通常没有行数据，此时 `payload` 为空但仍保存完整区间边界便于事后从源重读。
`retry_failed_batch()` 为兼容保留的空操作。Kafka retention gap 默认抛错，也可显式设置
`retention_policy="reset_to_earliest"`。

### Kafka offset 从哪里来、存到哪里

Kafka 没有“整个 topic 的一个 offset”，offset 永远属于某个 partition。监听器每轮从 broker 取得：

```text
partition -> (low, high)
```

`high` 表示下一个将被写入的 offset，因此某个 Handler 在该分区的积压量为：

```text
backlog = high - committed
独立模式 checkpoint key = handler_id:source_id:partition
共享模式 checkpoint key = shared:shared_source_group:source_id:partition
```

首次启动时先按 key 查询 checkpoint：存在则从它继续；不存在时，`initial_offset="earliest"`
以当前 low 为基线，`"latest"` 以当前 high 为基线。这个首次基线会立即持久化，避免程序在下一轮
观察前重启而跳过数据。随后 Dispatcher 将 `[committed, high)` 分成多个半开区间；只有一批中
所有 Ray task 都成功，才把该批 `end_offset` 原子写入 checkpoint。

这里故意不使用 Kafka consumer group 的 committed offset 作为调度真相。共享模式需要一组 Handler
共同推进一个 checkpoint；独立模式则需要各自推进。Postgres 也使用同一套模型。fetcher/worker 的
Consumer 使用 `assign(topic, partition, start_offset)` 精确读取请求区间，不依赖 group 当前位置。

共享模式使用新的 checkpoint key，不会自动合并已有的 Handler checkpoint。上线迁移时，应先确认旧的
Handler checkpoint 完全一致，再把该值写入新的 `shared:...` key；否则按 `initial_offset` 建立新基线可能
造成重放或跳过。

内置 `SQLiteCheckpointStore` 适合单个 Dispatcher 部署，并可跨进程重启恢复；生产多实例部署应将
`CheckpointStore` 替换为带 compare-and-set/事务能力的共享数据库实现，保证同一个 checkpoint key
只有一个调度所有者。`MemoryCheckpointStore` 只适合测试或一次性演示。

该方案提供现实可行的 **at-least-once** 语义。Dispatcher 或 Ray 故障时 task 可能重复执行，因此 worker
必须以 `dispatch_id` 幂等写入。若需要 exactly-once，业务输出与 checkpoint 必须落在同一事务边界，或由
下游提供幂等/事务能力。单实例可直接使用 SQLite；多实例生产部署应替换为 Postgres 等共享、原子存储。

## 验证

```bash
cd outputs
python3 -m pip install -e .
python3 -m unittest discover -s tests -v
python3 -m py_compile ray_dispatcher/*.py tests/test_ray_dispatcher.py
```

覆盖：Kafka 增量切分与整批提交、单次 fetch 多 Handler 扇出、Handler 重试复用 ObjectRef、原始 Ray
ObjectRef 轮询映射、SQLite 重启恢复、稳定 dispatch ID、Postgres 复合游标切片、retention gap 和循环启停。
