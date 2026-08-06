"""用户插件样例：过滤订单并返回结构化结果。

上传时文件名只要是安全的 ``.py`` 文件即可；示例 plugin_id 使用 ``orders_filter_v1``。
``handler_id`` 由平台按 ``{plugin_id}:{entrypoint}`` 生成，用户不要手写。

编写新插件时可以复制这个文件：
1. 上传时传入你的 plugin_id，用它标记这个 Worker 插件。
2. 修改 ``SOURCES``，声明你要读取的 Kafka/Postgres 来源。
3. 修改 ``HANDLERS``，让 ``entrypoint`` 指向当前文件里的处理函数。
4. Handler 返回一个简短的 dict 摘要；不要假设存在 output/sink 字段。
"""

from __future__ import annotations

from typing import Any


SOURCES = {
    # Source 名称在当前 worker 模块内声明；如果与内置 worker 或已生效插件同名，
    # 配置必须完全等价，否则 enable 时会被判定为冲突。
    #
    # Kafka 输入样式：
    "plugin-orders": {
        "kind": "kafka",
        "brokers": ["kafka-1:9092", "kafka-2:9092"],
        "topic": "orders",
        "initial_offset": "latest",
    },
    #
    # Postgres 输入样式一：cursor 模式，按 (timestamp_column, primary_key_column)
    # 维护增量进度，适合能稳定排序的业务表。
    #
    # "plugin-orders-pg": {
    #     "kind": "postgres",
    #     "dsn": "postgresql://user:password@pgbouncer-host:6432/appdb",
    #     "table": "public.orders",
    #     "timestamp_column": "updated_at",
    #     "primary_key_column": "id",
    #     "mode": "cursor",
    #     "initial_cursor": {
    #         "timestamp": "2026-01-01T00:00:00Z",
    #         "primary_key": 0,
    #     },
    # },
    #
    # Postgres 输入样式二：event_time 模式，按事件时间窗口推进，
    # 适合不能按主键稳定排序、但有事件时间字段的表。
    #
    # "plugin-orders-pg-event-time": {
    #     "kind": "postgres",
    #     "dsn": "postgresql://user:password@pgbouncer-host:6432/appdb",
    #     "table": "public.orders",
    #     "timestamp_column": "event_time",
    #     "mode": "event_time",
    #     "watermark_lag_seconds": 60,
    #     "min_window_seconds": 30,
    #     "max_window_seconds": 300,
    #     "max_rows": 10000,
    #     "initial_time": "2026-01-01T00:00:00Z",
    # },
}


RESOURCES = {
    # 可选。resources 适合小型静态配置，或平台统一加载的快照数据。
    # 只有 HANDLERS 中引用了 resources，处理函数签名才需要接收 resources 参数。
    "allowed-users-v1": {
        "kind": "static",
        "data": {
            "u1": {"tier": "gold"},
            "u2": {"tier": "silver"},
        },
    },
    #
    # 如果资源会低频变化，可以开启 TTL。到期后，框架会在下一次 handler
    # 提交前重新加载资源；不需要 reload worker。
    #
    # "rules-v1": {
    #     "kind": "file",
    #     "path": "/data/resources/rules.json",
    #     "refresh_policy": {"type": "ttl", "seconds": 60},
    # },
    #
    # "user-dim-v1": {
    #     "kind": "postgres",
    #     "dsn": "postgresql://user:password@pgbouncer-host:6432/appdb",
    #     "query": "select id, name, tier from public.users where status = 'active'",
    #     "key_column": "id",
    #     "formatter": "format_user_dim",
    #     "refresh_policy": {"type": "ttl", "seconds": 300},
    # },
}


def format_user_dim(rows: dict[Any, dict[str, Any]]) -> dict[Any, str]:
    """可选 resource formatter：资源加载后、进入缓存前执行一次。

    例如 Postgres resource 默认是 dict[id, row_dict]；如果 handler 只关心
    tier，可以在这里转换成 dict[id, tier]，避免每个 handler 重复转换。
    """

    return {user_id: row.get("tier", "unknown") for user_id, row in rows.items()}


HANDLERS = [
    {
        # 当前文件中的处理函数名。平台会用上传时的 plugin_id 自动生成：
        # orders_filter_v1:filter_orders
        # 所以这里不要写 handler_id，避免用户指定内部唯一标识。
        "entrypoint": "filter_orders",
        # 引用 SOURCES 中声明的名称，或平台注入的 source registry 名称。
        "sources": ["plugin-orders"],
        # 可选。引用 RESOURCES 中声明的名称，或平台注入的 resource registry 名称。
        "resources": ["allowed-users-v1"],
        # [触发下限, 单批上限]。这里表示 backlog >= 1 就触发，
        # 每次 dispatch 最多处理 1000 条记录。
        "batch_size": [1, 1000],
        "cpus_per_task": 1,
        "max_retries": 2,
        "priority": 0,
    }
]


def filter_orders(
    request: Any,
    records: list[dict[str, Any]],
    resources: dict[str, Any],
) -> dict[str, Any]:
    """处理一次 dispatch。

    常见函数签名：
    - 不使用 resources：handler(request, records)
    - 使用 resources：handler(request, records, resources)
    - 多源 Kafka：records 是 dict[source_id, list]
    """

    allowed_users = resources["allowed-users-v1"]
    filtered = [row for row in records if row.get("user_id") in allowed_users]

    return {
        "handler": request.handler_id,
        "input_count": len(records),
        "output_count": len(filtered),
        "accepted_user_ids": sorted(
            {
                str(row.get("user_id"))
                for row in filtered
                if row.get("user_id") is not None
            }
        ),
    }
