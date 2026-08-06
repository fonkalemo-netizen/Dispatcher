# Worker 插件上传、解析与接口说明

本文说明用户上传 Worker 插件时，服务端经过哪些模块处理、插件文件如何被解析成 Dispatcher 可调度的 Handler，以及 HTTP / RPC 两种管理接口的调用样例。

当前约定：上传物只支持单个安全的 `.py` 文件，文件名不要求是 `worker.py`；内置 Worker 仍来自 `workers/` 目录；用户插件进入独立的 `plugin_uploads/` / `plugin_active/` 目录，不直接写入内置目录。

## 1. 目录与元数据

插件状态由 `PluginStore` 维护，默认数据目录结构如下：

```text
data/
├── workers/                    # 内置 Worker，发布物，不接受用户上传
├── plugin_uploads/{plugin_id}/ # staging：上传后默认 disabled
│   └── {uploaded_filename}.py
├── plugin_active/{plugin_id}/  # active：enable 时校验通过后的生效候选副本
│   └── {uploaded_filename}.py
└── plugins.json                # 元数据唯一真相
```

`plugins.json` 中每个插件至少包含：

```json
{
  "plugins": {
    "orders_filter_v1": {
      "plugin_id": "orders_filter_v1",
      "desired_enabled": true,
      "effective_enabled": false,
      "reload_pending": true,
      "filename": "orders_filter.py",
      "sha256": "....",
      "validation": {
        "ok": true,
        "errors": [],
        "warnings": []
      },
      "handler_ids": ["orders_filter_v1:filter_orders"],
      "source_ids": ["plugin-orders"],
      "resource_ids": ["allowed-users-v1"]
    }
  }
}
```

三态含义：

| 字段 | 含义 |
|---|---|
| `desired_enabled` | 用户期望是否启用 |
| `effective_enabled` | Dispatcher 当前是否已经安装该插件 |
| `reload_pending` | 是否还有一次热更新等待执行或重试 |

删除插件必须满足：

```text
desired_enabled == false
effective_enabled == false
reload_pending == false
```

否则接口返回冲突，避免删除仍在运行或等待切换的 active 副本。

## 2. 核心模块与职责

| 模块 | 代码位置 | 职责 |
|---|---|---|
| `PluginStore` | `outputs/ray_dispatcher/plugins/store.py` | 管理 staging / active 目录、读写 `plugins.json`、上传、校验、晋升 active、删除、启动 reconcile、生成插件状态指纹 |
| `validate_worker_py()` | `outputs/ray_dispatcher/plugins/validate.py` | 对上传的 `.py` 文件做文件限制、AST 安全扫描、结构校验、子进程 discover 试加载 |
| `PluginManager` | `outputs/ray_dispatcher/plugins/manager.py` | 面向 HTTP / RPC 的编排层：upload / enable / disable / delete，并调用 `dispatcher.reload_workers()` |
| `create_plugin_router()` | `outputs/ray_dispatcher/plugins/api.py` | 可选 FastAPI 管理路由，薄封装 `PluginManager` |
| `discover_worker_roots()` | `outputs/ray_dispatcher/discovery.py` | 多目录 Worker 解析：内置目录 + 插件候选目录，合并 `SOURCES` / `RESOURCES` / `HANDLERS` |
| `RayDispatcher.reload_workers()` | `outputs/ray_dispatcher/dispatcher.py` | 在无 in-flight 时重扫 roots、预加载 resources、切换 worker/source/resource 配置 |

推荐集成方式：

```python
from pathlib import Path

from ray_dispatcher import RayDispatcher
from ray_dispatcher.plugins import PluginManager, PluginStore, create_plugin_router

store = PluginStore(Path("data"))
dispatcher = RayDispatcher(
    Path("data/workers"),
    plugin_store=store,
    reload_interval=5,
)
manager = PluginManager(store, dispatcher)
router = create_plugin_router(manager)
```

服务启动时建议执行：

```python
await manager.startup()
```

它会执行启动 reconcile，并尝试应用上次遗留的 pending reload。

## 3. 上传与启停流程

### 3.1 上传

```text
POST /api/plugins
  multipart:
    plugin_id=orders_filter_v1
    file=@orders_filter.py
```

内部流程：

1. `PluginStore.upload(plugin_id, content)`
2. 校验 `plugin_id` 与文件大小。
3. 写入 `plugin_uploads/{plugin_id}/{uploaded_filename}.py`，并在 metadata 中保存 `filename`。
4. 调用 `validate_worker_py()`。
5. 写入 `plugins.json`，默认：

```text
desired_enabled=false
effective_enabled=false
reload_pending=false
```

上传只进入 staging，不会进入 Dispatcher 生效集。

### 3.2 Enable

```text
POST /api/plugins/{plugin_id}/enable
```

内部流程：

1. `PluginStore.promote_active(plugin_id)` 再次校验 staging。
2. 校验通过后，原子晋升到 `plugin_active/{plugin_id}/{uploaded_filename}.py`。
3. `PluginManager` 探测与内置 Worker / 其它候选插件的 handler/source/resource 冲突。
4. 写入：

```text
desired_enabled=true
effective_enabled=false
reload_pending=true
```

5. 如果 Dispatcher 当前有 in-flight，返回 `pending_inflight`，不 swap。
6. 如果 Dispatcher 空闲，调用：

```python
await dispatcher.reload_workers(
    candidate_plugin_roots=store.list_candidate_plugin_roots()
)
```

7. swap 成功后由 `PluginStore.finalize_after_reload()` 写回：

```text
desired_enabled=true
effective_enabled=true
reload_pending=false
```

注意：`list_active_roots()` 只返回 `effective_enabled=true` 的插件。pending enable 的 active 文件即使已经在盘上，也不会通过 `list_active_roots()` 自动进入 Dispatcher；它只能作为显式 `candidate_plugin_roots` 参与下一次 reload。

### 3.3 Disable

```text
POST /api/plugins/{plugin_id}/disable
```

内部流程：

1. 写入用户意图：

```text
desired_enabled=false
reload_pending=true
```

2. 如果当前插件仍 `effective_enabled=true`，保留 active 文件，直到一次成功 swap 将它从 Dispatcher 移除。
3. swap 成功后：

```text
desired_enabled=false
effective_enabled=false
reload_pending=false
```

并删除 `plugin_active/{plugin_id}/`。

### 3.4 Delete

```text
DELETE /api/plugins/{plugin_id}
```

仅允许删除完全 disabled 且无 pending 的插件。否则返回 `409`。

## 4. 插件 `.py` 编写与解析规则

插件文件必须是安全文件名的 `.py` 文件，必须导出非空 `HANDLERS`。`SOURCES` / `RESOURCES` 可选。用户上传时提供的 `plugin_id` 用来标记这个 Worker 插件；Handler 的内部唯一 ID 由平台生成。

最小形态：

```python
SOURCES = {
    "plugin-orders": {
        "kind": "kafka",
        "brokers": ["kafka-1:9092"],
        "topic": "orders",
        "initial_offset": "latest",
    }
}

HANDLERS = [
    {
        "entrypoint": "filter_orders",
        "sources": ["plugin-orders"],
        "batch_size": [1, 1000],
    }
]


def filter_orders(request, records):
    return {
        "handler": request.handler_id,
        "input_count": len(records),
    }
```

带 resource 的形态：

```python
RESOURCES = {
    "allowed-users-v1": {
        "kind": "static",
        "data": {
            "u1": {"tier": "gold"},
            "u2": {"tier": "silver"},
        },
    }
}

HANDLERS = [
    {
        "entrypoint": "filter_orders",
        "sources": ["plugin-orders"],
        "resources": ["allowed-users-v1"],
        "batch_size": [1, 1000],
    }
]


def filter_orders(request, records, resources):
    allowed_users = resources["allowed-users-v1"]
    filtered = [row for row in records if row.get("user_id") in allowed_users]
    return {
        "handler": request.handler_id,
        "input_count": len(records),
        "output_count": len(filtered),
    }
```

Postgres source 样式：

```python
SOURCES = {
    "plugin-orders-pg": {
        "kind": "postgres",
        "dsn": "postgresql://user:password@pgbouncer-host:6432/appdb",
        "table": "public.orders",
        "timestamp_column": "updated_at",
        "primary_key_column": "id",
        "mode": "cursor",
        "initial_cursor": {
            "timestamp": "2026-01-01T00:00:00Z",
            "primary_key": 0,
        },
    }
}
```

`handler_id` 规则：

```text
上传插件中不要声明 handler_id；平台按 "{plugin_id}:{entrypoint}" 生成
```

例如上传时 `plugin_id=orders_filter_v1`，并且 `entrypoint=filter_orders`，内部生成：

```text
orders_filter_v1:filter_orders
```

用户插件中不应该写：

```text
"handler_id": "orders_filter_v1:filter_orders"
```

完整可复制样例见：

```text
outputs/examples/plugins/orders_filter_v1/worker.py
```

## 5. 校验规则

`validate_worker_py()` 做四层校验：

1. 文件校验：必须是 `.py`，文件名不能包含路径穿越，非空，不超过大小上限。
2. AST 扫描：拒绝危险 import / 调用。
3. 结构校验：检查 `HANDLERS`、entrypoint 是否存在、source/resource 引用；handler_id 由平台生成。
4. 子进程 discover：在隔离子进程中调用 `discover_workers(staging_dir)`，超时杀掉。

允许的常见 import：

```text
json, csv, io, pathlib, typing, datetime, decimal, math, collections, dataclasses, enum, functools, itertools, re ...
```

禁止的常见能力：

```text
eval, exec, compile, __import__
os, sys, subprocess, socket, ctypes, importlib, multiprocessing, threading, shutil
os.system, os.popen, os.remove, os.unlink, os.rename, os.replace
Path.unlink, Path.rmdir
```

受控写盘策略：

- 明显危险路径如 `/etc`、`/root`、`/proc`、`/sys`、`/dev`、`~/.ssh`、`..` 穿越会拒绝。
- 其它硬编码绝对路径第一版只给 warning。
- AST 不假装证明路径一定在某个 output 根下；最终隔离依赖容器、挂载和运行时权限。

当前官方插件样例不依赖 `request.output` / sink；handler 返回结构化 dict 结果。

## 6. Dispatcher 解析与热更新

插件最终通过 `discover_worker_roots()` 进入 Dispatcher。

正常生效集：

```python
dispatcher_roots = [
    builtin_workers_dir,
    *plugin_store.list_active_roots(),
]
```

热更新候选集：

```python
candidate_roots = plugin_store.list_candidate_plugin_roots()

await dispatcher.reload_workers(
    candidate_plugin_roots=candidate_roots
)
```

`reload_workers()` 内部做：

1. `discover_workers([builtin, *candidate_plugin_roots])`
2. 合并并解析 `SOURCES` / `RESOURCES` / `HANDLERS`
3. 计算 dispatcher config fingerprint
4. 如果 fingerprint 未变化，返回 `False`
5. 如果有 in-flight，返回 `False`
6. 预加载 ResourceLoader
7. 再次确认无 in-flight
8. swap `workers/source_registry/resource_registry/resource_loader`
9. `PluginStore.finalize_after_reload(set(dispatcher.workers))`

两个指纹分工：

| 指纹 | 内容 | 用途 |
|---|---|---|
| dispatcher config fingerprint | 内置 Worker sha、已安装插件 sha、canonical handlers/sources/resources | 判断 Dispatcher 是否需要 swap |
| plugin store fingerprint | desired/effective/pending、staging sha、active sha、validation | HTTP 状态、审计、排查 |

## 7. HTTP 接口样例

下面假设服务地址是 `http://localhost:8000`。

### 上传插件

```bash
curl -sS -X POST "http://localhost:8000/api/plugins" \
  -F "plugin_id=orders_filter_v1" \
  -F "file=@outputs/examples/plugins/orders_filter_v1/worker.py"
```

典型响应：

```json
{
  "plugin_id": "orders_filter_v1",
  "desired_enabled": false,
  "effective_enabled": false,
  "reload_pending": false,
  "filename": "worker.py",
  "validation": {
    "ok": true,
    "errors": [],
    "warnings": []
  },
  "handler_ids": ["orders_filter_v1:filter_orders"],
  "source_ids": ["plugin-orders"],
  "resource_ids": ["allowed-users-v1"],
  "plugin_store_fingerprint": "{\"plugins\":[...]}"
}
```

### 查看插件列表

```bash
curl -sS "http://localhost:8000/api/plugins"
```

### 查看单个插件

```bash
curl -sS "http://localhost:8000/api/plugins/orders_filter_v1"
```

### 启用插件

```bash
curl -sS -X POST "http://localhost:8000/api/plugins/orders_filter_v1/enable"
```

空闲时典型响应：

```json
{
  "plugin_id": "orders_filter_v1",
  "desired_enabled": true,
  "effective_enabled": true,
  "reload_pending": false,
  "reload_result": "swapped"
}
```

有 in-flight 时响应状态码为 `202`：

```json
{
  "plugin_id": "orders_filter_v1",
  "desired_enabled": true,
  "effective_enabled": false,
  "reload_pending": true,
  "reload_result": "pending_inflight"
}
```

### 禁用插件

```bash
curl -sS -X POST "http://localhost:8000/api/plugins/orders_filter_v1/disable"
```

如果当前有 in-flight，会保持：

```json
{
  "desired_enabled": false,
  "effective_enabled": true,
  "reload_pending": true,
  "reload_result": "pending_inflight"
}
```

等下一次空闲 reload 成功后，变为：

```json
{
  "desired_enabled": false,
  "effective_enabled": false,
  "reload_pending": false,
  "reload_result": "swapped"
}
```

### 删除插件

```bash
curl -sS -X DELETE "http://localhost:8000/api/plugins/orders_filter_v1"
```

成功：

```json
{
  "plugin_id": "orders_filter_v1",
  "status": "deleted"
}
```

如果插件仍 enabled 或 pending，返回 `409`。

## 8. RPC / 进程内调用样例

如果不想暴露 HTTP，可以直接把 `PluginManager` 封成 RPC 服务。核心 API 与 HTTP 完全一致。

```python
from pathlib import Path

from ray_dispatcher import RayDispatcher
from ray_dispatcher.plugins import PluginManager, PluginStore


async def build_plugin_manager() -> PluginManager:
    store = PluginStore(Path("data"))
    dispatcher = RayDispatcher(
        Path("data/workers"),
        plugin_store=store,
        reload_interval=5,
    )
    manager = PluginManager(store, dispatcher)
    await manager.startup()
    return manager


async def upload_and_enable(manager: PluginManager) -> dict:
    content = Path("outputs/examples/plugins/orders_filter_v1/worker.py").read_bytes()
    await manager.upload("orders_filter_v1", content)
    result = await manager.enable("orders_filter_v1")
    return manager.status_payload(
        result.record,
        reload_result=result.reload_result,
    )
```

RPC 层建议只做薄封装：

- 权限校验、审计日志、租户隔离放在 RPC / HTTP 网关层。
- 插件状态变更仍统一调用 `PluginManager`。
- 不要绕过 `PluginStore.promote_active()` 直接写 `plugin_active/`。
- 不要绕过 `dispatcher.reload_workers(candidate_plugin_roots=...)` 自己 import 插件。

## 9. 常见失败与返回语义

| 场景 | 结果 |
|---|---|
| 上传文件不是 `.py` 或文件名不安全 | 接口 `400` 或 `validation.ok=false` |
| AST 命中危险 API | `validation.ok=false`，不得 enable |
| 用户在 HANDLERS 中手写 handler_id | `validation.ok=false` |
| 与内置或已启用插件 handler/source/resource 冲突 | enable 返回 `failed_conflict` |
| enable 时有 in-flight | 返回 `pending_inflight`，metadata 保持 pending，等待下次 reload |
| discover 失败 | 返回 `failed_discover`，pending 保留，方便修复后重试 |
| resource preload 失败 | 返回 `failed_preload`，不推进生效状态 |
| delete enabled/pending 插件 | `409` |

## 10. 推荐排查入口

- 看插件状态：`GET /api/plugins/{plugin_id}`
- 看 staging 文件：`data/plugin_uploads/{plugin_id}/{filename}`
- 看 active 文件：`data/plugin_active/{plugin_id}/{filename}`
- 看元数据：`data/plugins.json`
- 看 Dispatcher 错误：`dispatcher.state.loop_errors`
- 看测试覆盖：`outputs/tests/test_plugins.py`
