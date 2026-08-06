# RayDispatcher

`ray_dispatcher` 包提供单次轮询函数及对应的后台循环：

- `data_listener()`：读取 Kafka 分区 high watermark，或 Postgres 稳定复合游标和窗口 count。
- `ray_trigger()`：结合 backlog、Handler 并发限制、全局 in-flight 限制和当前可用 CPU，生成 Ray task/Actor 调用。
- `ray_status()`：非阻塞轮询 ObjectRef，失败重试；永久失败写入 FailureStore 后跳过区间并推进 checkpoint。
- `event_log_tick()`：周期发出 `snapshot` 事件（可用 `event_log_interval<=0` 关闭）。
- `reload_workers()`：目录模式下重扫 workers；`reload_interval>0` 时由 `start()` 定时调用（有 in-flight 时推迟切换）。
- `start()` / `stop()`：定时并发运行上述循环（含可选的 event_log / reload）。

包布局：

| 模块 | 职责 |
|---|---|
| [`ray_dispatcher/dispatcher.py`](./ray_dispatcher/dispatcher.py) | 调度核心 |
| [`ray_dispatcher/discovery.py`](./ray_dispatcher/discovery.py) | workers 目录扫描 |
| [`ray_dispatcher/registries.py`](./ray_dispatcher/registries.py) | 命名 source / resource 注册表 |
| [`ray_dispatcher/readers.py`](./ray_dispatcher/readers.py) | 框架侧 PayloadReader（fetch） |
| [`ray_dispatcher/resources.py`](./ray_dispatcher/resources.py) | worker 作用域 ResourceLoader |
| [`ray_dispatcher/sources.py`](./ray_dispatcher/sources.py) | 固定 SourceObserver（水位/切分） |
| [`ray_dispatcher/adapter.py`](./ray_dispatcher/adapter.py) | Native Ray 执行适配 |
| [`ray_dispatcher/checkpoint.py`](./ray_dispatcher/checkpoint.py) | 进度 + 可选 Actor state |
| [`ray_dispatcher/event_log.py`](./ray_dispatcher/event_log.py) | 运行事件钩子（函数登记） |
| [`ray_dispatcher/failures.py`](./ray_dispatcher/failures.py) | 永久失败区间落盘 |
| [`ray_dispatcher/policy.py`](./ray_dispatcher/policy.py) | 渐进式调度策略 |
| [`ray_dispatcher/plugins/`](./ray_dispatcher/plugins/) | 用户 Worker 插件上传 / 校验 / 热启停 |

调度核心没有强制第三方依赖；生产适配器按需加载。可用 `pip install -e .` 安装本包。
HTTP 管理面可选：`pip install -e ".[api]"`（FastAPI）。

## Worker 插件热启停

用户上传的 `.py` 不进内置 `workers/`：经 `plugin_uploads/` staging → AST+子进程校验 → enable 时拷贝到 `plugin_active/` → `reload_workers()`。

- 状态：`desired_enabled` / `effective_enabled` / `reload_pending`（见 `plugins.json`）
- `list_active_roots()` **只**返回 `effective_enabled=true`；pending enable 由 `list_pending_enable_roots()` / `list_candidate_plugin_roots()` 交给 `reload_workers(candidate_plugin_roots=...)`
- metadata 只在 swap 成功后 `finalize_after_reload`；失败不 rollback
- 当前插件样本不假设 `output` / sink 字段；Handler 返回结构化结果即可
- AST 仍支持受控写盘校验策略：禁 `subprocess`/`eval`/…；危险绝对路径拒绝，其它绝对路径 warning。若平台后续向插件开放 sink，应由运行时权限保证只能写 output 根
- API：`PluginManager` + `create_plugin_router()`（`POST/GET /api/plugins`，enable/disable/delete）
- 样本：[`examples/plugins/orders_filter_v1/worker.py`](./examples/plugins/orders_filter_v1/worker.py)
- 详细上传、解析模块和接口样例：[`docs/plugin-upload-and-discovery.md`](./docs/plugin-upload-and-discovery.md)

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

`workers/events.py` 只声明薄 Handler：来源与资源用注册表里的名字引用。相同 `source_id` 的
Handler 自动共享一次 fetch 与同一个 checkpoint。`entrypoint` 指向函数名；调度 ID 默认为
`{文件名}:{entrypoint}`（可用 `handler_id` 覆盖）。

**单源**（默认）：按 partition offset 调度；`batch_size` 是触发门槛 + 本批上限区间，
Dispatcher **不按 batch_size 切多片**（每轮每源最多一波 fetch）。`handler(request, records)` 中
`records` 为 `list`；若需再分片在 Handler 内自行处理。

`batch_size` 写法：

| 配置 | 含义 |
|---|---|
| `n` 或 `(n, None)` | `[n,]`：`backlog >= n` 才触发；本批取全部未处理 |
| `(None, n)` | `[,n]`：有数据即触发；本批最多 `n`，剩余下轮 |
| `(n, m)` | `[n,m]`：`backlog >= n`；本批最多 `m` |
| `0` | 等价 `(0, None)`：有 backlog 就一次吃光 |

**多源**（`sources` 长度 ≥ 2，且必须全是 Kafka）：按 **event-time 时间窗** 对齐。右界来自
`data_listener` 观察到的各源可对齐水位取 `min`，左界为组级 checkpoint
`mswin:ms:{id1}+{id2}`，单批再被 `DispatcherConfig.max_window_seconds`（默认 60）截断。
`offsets_for_times` 缺某分区映射时回退到该分区 **high**（空区间），不会回退到 low。
一次窗口内对各源各分区 fetch → merge 为 `dict[source_id, list]` → 扇出 Handler；全部成功才推进时间窗与各分区 offset。

```python
SOURCES = {
    "events-v1": {
        "kind": "kafka",
        "brokers": ["kafka-1:9092", "kafka-2:9092"],
        "topic": "events",
        "initial_offset": "latest",
    }
}

RESOURCES = {
    "user-dim-v1": {
        # static: small inline config only; large dims use kind "file"
        # postgres snapshots are also supported, e.g.:
        # "user-dim-v1": {
        #   "kind": "postgres",
        #   "dsn": "postgresql://...",
        #   "table": "users",          # or "query": "select ..."
        #   "key_column": "id",
        # },
        "kind": "static",
        "data": {},
    }
}

HANDLERS = [
    {
        # Binding
        "entrypoint": "events_to_jsonl",
        "sources": ["events-v1"],
        "resources": ["user-dim-v1"],
        # Write-side passthrough (opaque to the dispatcher)
        "output": {
            "path": "events/jsonl/",
        },
        # Scheduling knobs (write them out so they stay discoverable)
        "batch_size": [1, 50_000],  # 同组取最严：max(min) / min(max)
        "cpus_per_task": 1,
        "max_retries": 2,
        "priority": 0,
    },
    {
        "entrypoint": "events_to_csv",
        "sources": ["events-v1"],
        "output": {
            "path": "events/csv/",
        },
        "batch_size": [1, 50_000],
        "cpus_per_task": 1,
        "max_retries": 2,
        "priority": 0,
    },
]


def events_to_jsonl(request, records, resources):
    return write_jsonl_range(request, records, resources)


def events_to_csv(request, records):
    return write_csv_range(request, records)
```

多源 Kafka 示例：

```python
HANDLERS = [
    {
        "entrypoint": "join_orders_payments",
        "sources": ["orders", "payments"],
        "batch_size": [1, 10_000],  # 单源门槛/上限；多源另受时间窗约束
    },
]


def join_orders_payments(request, records):
    # records == {"orders": [...], "payments": [...]}
    ...
```

Worker 模块可导出 `HANDLERS` 以及旁路 `SOURCES` / `RESOURCES`（纯配置，无 callable）。
无 `HANDLERS` 或 `HANDLERS = []` 的 `.py` 会被跳过（仍可贡献 SOURCES/RESOURCES）；
目录内最终至少一个 Handler，否则启动失败。
启动扫描时先合并各模块声明（可选构造注入 registry 为底表；同名且配置不等价则失败），再统一解析
Handler 名字。

- **Source**：增量数据、Dispatcher 追进度（观察 / fetch / checkpoint）。
- **Resource**：快照旁路依赖；`static` 仅小配置，大维表用 `file`，也可用 `postgres`（全表或 SQL 一次快照）。注册表**合并完成后**由 `ResourceLoader.preload()` 统一加载进 cache；submit 只读 cache。
- **output**：可选透传 mapping → `HandlerRequest.output`；框架不解释。目录投放场景下业务依赖宜函数内
  import，写出路径走 `output`。
- Handler 收到瘦 `HandlerRequest`（`dispatch_id` / `handler_id` / `output`；Actor 首次
  submit 可有 `checkpoint_state`）；调度区间留在 `DispatchRequest` 供 fetch 使用。
- **fetch / 多源窗口**：`fetch_cpus`、`fetch_max_retries`、`max_window_seconds` 在
  `DispatcherConfig`（Dispatcher 构造参数）上配置，不写在 HANDLERS 里。
- **触发策略**：`TriggerPolicy` 由主系统注入（默认链：有 backlog → 源空闲 → 有容量）；
  输出 `TRIGGER | DEGRADE | SKIP | BLOCK`，条件还可附带 `SoftAction`
  （如 `THROTTLE` 会按 `concurrency_factor` 缩小本轮 `n`）。Handler / HANDLERS 不配置触发条件。

```python
import asyncio
from pathlib import Path

import ray

from ray_dispatcher import (
    DispatcherConfig,
    RayDispatcher,
    SQLiteCheckpointStore,
    SQLiteFailureStore,
)


async def main():
    ray.init()
    async with RayDispatcher(
        Path(__file__).parent / "workers",
        checkpoint_store=SQLiteCheckpointStore("dispatcher-checkpoints.sqlite3"),
        failure_store=SQLiteFailureStore("dispatcher-failures.sqlite3"),
        config=DispatcherConfig(max_in_flight=64),
    ) as dispatcher:
        await asyncio.Event().wait()


asyncio.run(main())
```

启动过程为：扫描所有 `workers/*.py` → 合并 `SOURCES` / `RESOURCES` → 展开 `HANDLERS` 并解析
名字 → 每个普通函数自动包装成独立 Ray remote function。区间读取由框架
`PayloadReader`（或注入的自定义 reader）完成，Handler 只处理已读出的 `records`。
`ray_adapter` 为关键字参数且可省略；水位观察固定为包内 `SourceObserver`。也可用
`async with dispatcher` 代替手动 `start()` / `stop()`。

之后每次 `data_listener()` 都会主动：

- 调用 Kafka `list_topics()` 发现分区；
- 对每个分区调用 `get_watermark_offsets(..., cached=False)` 获取实时 low/high offset；
- 和 Dispatcher 持有的 committed checkpoint 比较，得到 backlog；
- 对 Postgres 查询最大 `(timestamp_column, primary_key_column)`，并统计游标窗口内的 count。

这里的 offset 不是 worker 传进来的。worker 只声明关注哪些**命名**来源，实际 offset 由
`SourceObserver` 主动向 broker 查询，然后由 Dispatcher 切分并走 fetch→fanout。

生产数据源实现需要可选依赖：

```bash
pip install ray confluent-kafka asyncpg
```

完整可运行的双 Handler 示例见 [example_app](./example_app/README.md)：同一 `demo-orders`
来源只 fetch 一次，通过 Ray ObjectRef 分别输出 JSONL 和 CSV，并包含共享 checkpoint 与批量拆分。

## 自动共享：一次 fetch，多 Handler 处理

所有 Handler 都走 fetch→fanout。声明相同 `source_id`（注册表名字）的 Handler 自动共享：

- 一个 source checkpoint（`shared:{source_id}:{shard}`）
- 每个 offset slice 一次框架 fetch
- 同一 ObjectRef 扇出给组内所有 Handler

Handler 签名为 `handler(request, records)`，需要快照时再加 `resources`：
`handler(request, records, resources)`（Task）。Dispatcher 的执行顺序是：

```text
一个 offset slice
  -> 一个 fetch task（PayloadReader.fetch）
  -> 一个 Ray ObjectRef
  -> N 个 Handler task（records ObjectRef 顶层第二参数；
     resources 经 Driver ray.put 一次后以共享 ObjectRef 注入）
  -> N 个 Handler 全部成功
  -> 推进共享 checkpoint
```

Actor 模式：`resources` 在 Actor `__init__(resources)` 注入一次，
`process(request, records)` 不再传 resources，应从 `self` 读取。

Handler 业务失败重试会复用原 fetch ObjectRef，不会再次读取 Kafka。fetch 自身失败则按
`DispatcherConfig.fetch_max_retries` 重试；永久失败会先写入 `FailureStore`（区间元信息 + 可物化的 fetch payload），
再跳过该区间、推进 checkpoint，并解除来源阻塞。成功提交 checkpoint 后 Dispatcher 会释放本批
ObjectRef，避免历史数据长期占用 Object Store。

同 `source_id` 的 Handler 必须共用一个物理来源配置与相同的 `initial_offset` /
`retention_policy`（或 Postgres `initial_cursor`）。fetch 配额与重试由 Dispatcher 的
`DispatcherConfig` 统一配置。单源 Handler 声明一个 source；多源 Kafka
（`sources` 长度 ≥ 2）走时间窗路径，组级 checkpoint 为 `mswin:…`。

以下手工构造方式适合动态生成 Handler（不经目录 discovery）。仍走框架
fetch→fanout：`PayloadReader` 读区间，Handler 只处理已读出的 `records`。

## 手工构造方式

```python
import asyncio

import ray

from ray_dispatcher import (
    DispatcherConfig,
    HandlerSpec,
    KafkaSource,
    RayDispatcher,
    SQLiteCheckpointStore,
    SQLiteFailureStore,
)


@ray.remote(max_retries=0)  # 手工构造需自行包装；目录 discovery 会代劳
def normalize_events(request, records):
    """业务 Handler：接收瘦 HandlerRequest + 已 fetch 的 message values。"""
    # request: dispatch_id / handler_id / output（Actor 首次可有 checkpoint_state）
    # records: list[value]；不要在这里再开 Kafka Consumer
    for index, value in enumerate(records):
        persist_event(
            payload=value,
            event_key=f"{request.dispatch_id}:{index}",
            dispatch_id=request.dispatch_id,
        )
    return {"processed": len(records)}


source = KafkaSource(
    source_id="events-v1",
    brokers=("kafka-1:9092", "kafka-2:9092"),
    topic="events",
    initial_offset="latest",  # 首次启动不处理历史；改成 earliest 可回放
)

handler = HandlerSpec(
    name="event-normalizer",
    worker=normalize_events,
    sources=(source,),
    batch_size=(1, 50_000),  # 或整数 50000 表示 [50000,]
    cpus_per_task=1,
)


async def main():
    ray.init()
    async with RayDispatcher(
        workers=(handler,),
        # ray_adapter 可省略；默认 NativeRayAdapter。水位观察固定为 SourceObserver
        # 区间读取默认 CompositePayloadReader（KafkaPayloadReader / PostgresPayloadReader）
        checkpoint_store=SQLiteCheckpointStore("dispatcher-checkpoints.sqlite3"),
        failure_store=SQLiteFailureStore("dispatcher-failures.sqlite3"),
        config=DispatcherConfig(max_in_flight=64),
        operation_timeout=30,
    ) as dispatcher:
        await asyncio.Event().wait()


asyncio.run(main())
```

Dispatcher 决定“哪个分区、从哪里开始、到哪里结束”，并由框架 `PayloadReader`
（默认 `KafkaPayloadReader` / `PostgresPayloadReader`）完成精确区间读取；Handler 只解析/
写出 `records`。Kafka 路径下不要在 Handler 里自行 `poll` 或 `consumer.commit()`；
slice 成功后由 Dispatcher 推进 checkpoint。自定义读取逻辑应实现/注入 `PayloadReader`，
而不是写进业务 Handler。

`DispatchRequest` 供 fetch / merge / 内部状态使用：Kafka 含 `topic`、`partition`、
offset 边界；Postgres 含 `table`、游标边界；多源时间窗还可带
`window_start` / `window_end` / `source_ids`。业务 Handler 拿到的是
瘦 `HandlerRequest`，不是 `DispatchRequest`。

Handler 签名为 `handler(request, records)`；声明了 `resources` 时 Task 为
`handler(request, records, resources)`（第三参为共享 ObjectRef 解引用后的 dict）。
Actor 应在 `__init__(self, resources)` 保存快照，`process(self, request, records)`
不再接收 resources。Kafka 路径下 `records` 为 message value 列表
（多源为 `dict[source_id, list[value]]`）；partition/offset 只用于 Dispatcher 调度与 checkpoint，
不塞进 payload。下游幂等由业务唯一键负责。

`HANDLERS` 里的 `output` 是可选的**透传 mapping**（例如 `{"path": "..."}` 或带 `uri`），进入瘦
`HandlerRequest.output`；Dispatcher 不解释、不连接。输入仍只由 `sources` / Dispatcher 管理。
业务 Handler 使用 `dispatch_id`、`handler_id`、`output`（及可选 `checkpoint_state`）与
`records`/`resources`。

## SourceObserver（固定水位观察）

水位观察与 Postgres 区间规划由包内固定的 `SourceObserver` 完成，不是可插拔扩展点；区间数据由
框架 `PayloadReader`（或注入的自定义 reader）读取，Handler 只处理 `records`。`SourceObserver` 会：

1. 查询 Kafka 分区 low/high watermark；
2. 查询 Postgres 稳定上界游标，并统计窗口 count（`mode=cursor`）；
3. 在数据库内按 `(timestamp, primary_key)` 排序，为本批上限（`batch_size` 区间的 max，或全部 backlog）
   生成**单一**有序游标范围（每个 wave 一个 fetch，不再按 batch_size 切多 task）。

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

#### `mode=event_time`（无 ORDER BY）

当存储不能排序时，配置 `mode: event_time`（默认仍是 `cursor`，兼容现有部署）：

| 字段 | 默认 | 含义 |
| --- | --- | --- |
| `watermark_lag_seconds` | `60` | `W = now(UTC) - lag` |
| `max_window_seconds` | `300` | 单窗最大时长 |
| `min_window_seconds` | `60` | 二分缩窗下限 |
| `max_rows` | `100000` | 目标行数上限；触底仍超则整窗拉取并 `window_over_capacity` |
| `initial_time` | — | 无 checkpoint 时必填 |

- 观察：用 `min(ts)` 跳过空洞；谓词 `(L, R]`，**无 ORDER BY**。
- 调度：`batch_size` **只**用于 Handler 内存分片（典型 `10000`），不是 trigger 的 min 门闩。
- 读取：`SELECT * WHERE ts > $1 AND ts <= $2` 一次；再 `take_slice` 扇出给 Handler。

## Task 还是 Actor

- 默认用 Task：区间相互独立、无状态、易扩缩。
- 用 Actor：需要缓存模型/历史数据、复用连接、维持分区有序状态。配置
  `mode=ExecutionMode.ACTOR, remote_method="process"` 后，每个 Handler 只创建并复用
  **一个** Actor；in-flight method 数由全局 slot / CPU 限制。
- **Resources（维表快照）传递：**
  - Task：Driver `preload` 后对每个 Handler 的 `resource_ids` 集合 `ray.put` **一次**，
    各次 submit 只传同一 ObjectRef（与 records 一样由 Ray 解析），避免每 task 拷整表。
  - Actor：构造时 `Actor.remote(resources)` 注入一次；`process(request, records)`
    **不再**带 resources，业务从实例字段读取。热更新会 `drop_actor` 并换新
    `resource_loader`（同时清空 put 缓存）。
  - 目录 discovery 会校验签名：Task±resources 参数个数、Actor 必须是类、
    `__init__(resources?)` / `process(request, records)` 约定；已 `@ray.remote` 包装的对象跳过。
  - `checkpoint_state` 只适合小业务状态，不要用来恢复大维表。
- 影响正确性的历史状态不能只放 Actor 内存。框架把 Actor 缓存写成独立 checkpoint 文档
  （见下节），并在 Actor（重新）创建后的首次 submit 经
  `HandlerRequest.checkpoint_state` 注入恢复。
- Handler 若需要持久化缓存，在返回的 `dict` 里带上 `checkpoint_state`（其余字段仍是业务结果）。
  Dispatcher 在整批成功时与进度同事务写入；毒区间 skip 只推进进度，不改 Actor state
  （state 的 as-of 可能暂时落后于 progress）。

## 状态、失败与一致性

所有运行状态都保存在 `dispatcher.state`：

- `state.sources`：committed/observed cursor、backlog、到达和处理速率。
- `state.batches`：来源窗口及整批状态。
- `state.runs`：每个 task 的状态、attempt、结果、错误和 Ray ref。
- `await dispatcher.snapshot()`：在调度锁下取景，适合日志或监控输出的无敏感连接信息快照。
- 目录启动可用 `reload_interval>0` 热加载 workers（有 in-flight batch 时推迟切换）。
- `event_log`：批提交 / 跳过 / run 永久失败 / 周期 snapshot 的函数钩子（见下节）。

构造时 `ray_adapter` 可省略（默认 `NativeRayAdapter`）；水位观察固定使用 `SourceObserver`。
失败落盘默认 `MemoryFailureStore`，生产可用 `SQLiteFailureStore`。测试/demo 可通过赋值
`dispatcher.source_observer` 替换观察器（鸭子类型），不作为正式扩展 API。

### 运行事件钩子（event_log）

默认 `RayDispatcher` 会安装内置 logging 钩子（logger：`ray_dispatcher.event_log`）。
事件：`batch_committed`、`batch_skipped`、`run_failed`、`snapshot`、`trigger_evaluated`。

用普通函数扩展（无需实现类）：

```python
from ray_dispatcher import EventLog, create_event_log, discover_event_hooks

events = create_event_log(default_logging=True)

def alert_skip(payload):
    # 二次加工 / 外发；慢 IO 请自行异步，勿阻塞调度
    ...

events.register("batch_skipped", alert_skip)
dispatcher = RayDispatcher(workers, event_log=events, event_log_interval=5.0)
# event_log_interval <= 0 关闭周期 snapshot tick
```

或从目录加载模块级 `HOOKS`：

```python
# hooks/alerts.py
HOOKS = [{"event": "batch_skipped", "entrypoint": "alert_skip"}]

def alert_skip(payload):
    ...

events = discover_event_hooks("hooks/")
```

也可用 `@hook("batch_skipped")` 登记到进程默认 registry，再 `event_log=default_event_log()` 注入。
Hook 异常写入 `state.loop_errors`，不打断调度。载荷为小 dict，不含 records / actor state。

调度排序为渐进字典序：静态 priority → backlog 最多 → 最慢（积压耗时）→ 等待最久；
然后再检查 slot / CPU。checkpoint 在一批全部成功时推进；永久失败时先写入
`FailureStore`（区间、错误摘要，并尽量物化 fetch payload），再推进到
`batch.end`（跳过毒区间）并清除 `active_batch_id`，来源可继续往后调度。
永久失败区间写入 FailureStore 后直接 skip 推进；如需重放，根据 FailureStore 中的
区间/payload 手动回退 checkpoint 后再调度。Kafka retention gap 默认抛错，也可显式设置
`retention_policy="reset_to_earliest"`。

### Kafka offset 从哪里来、存到哪里

Kafka 没有“整个 topic 的一个 offset”，offset 永远属于某个 partition。监听器每轮从 broker 取得：

```text
partition -> (low, high)
```

`high` 表示下一个将被写入的 offset，因此某个来源在该分区的积压量为：

```text
backlog = high - committed
checkpoint key = shared:{source_id}:{partition}
```

相同 `source_id` 的 Handler 共用这一 checkpoint；单 Handler 同样走 fetch 路径，也使用该 key。

首次启动时先按 key 查询 checkpoint：存在则从它继续；不存在时，`initial_offset="earliest"`
以当前 low 为基线，`"latest"` 以当前 high 为基线。这个首次基线会立即持久化，避免程序在下一轮
观察前重启而跳过数据。随后 Dispatcher 将 `[committed, high)` 分成多个半开区间；只有一批中
所有 fetch 与 Handler 都成功，才把该批 `end_offset` 原子写入 checkpoint。

Checkpoint 存的是文档 `CheckpointDocument(progress, state=None)`：

- 共享进度 key（`shared:…` / `mswin:…`）只写 `progress`，`state` 恒为 `None`。
- Actor 缓存写独立 key `actor:{handler_id}:{progress_key}`，文档内带 as-of `progress` 与
  不透明 `state`（须 JSON 可序列化）。批成功时用 `save_many` 与进度同事务提交。

这里故意不使用 Kafka consumer group 的 committed offset 作为调度真相。共享与单 Handler 都需要
Dispatcher 持有的 checkpoint；Postgres 也使用同一套模型。框架 PayloadReader 的
Consumer 使用 `assign(topic, partition, start_offset)` 精确读取请求区间，不依赖 group 当前位置。

若从旧版独立 Handler checkpoint（`handler_id:source_id:partition`）或旧共享 key
（`shared:group:source_id:partition`）迁移，应先确认旧值一致后写入新的 `shared:{source_id}:...`
key；否则按 `initial_offset` 建立新基线可能造成重放或跳过。

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
ObjectRef 轮询映射、SQLite 重启恢复、稳定 disatch ID、Postgres 复合游标切片、retention gap 和循环启停。
