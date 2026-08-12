"""内置 worker 示例：Kafka bytes -> msgspec Struct -> 规则打标 -> 新 Struct -> Kafka。

把这个文件复制到你的内置 ``workers/`` 目录即可被 ``discover_workers`` 扫描。

依赖：
    pip install msgspec confluent-kafka

说明：
    - 这个示例面向内置 worker，不是上传插件。
    - 内置 Kafka handler 收到的 ``records`` 是原始 ``message.value()``，
      通常是 ``list[bytes]``。
    - 这里用 msgspec 直接把 bytes 解码成 Struct，转换后再编码为 JSON bytes，
      最后通过 Kafka Producer 发到下游 topic。
    - 规则只写在 RESOURCES 里，handler 通过平台注入的 EqRuleLabeler 做等值匹配，
      不需要在业务代码里复制 if/else。
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

import msgspec
from confluent_kafka import Producer


INPUT_BROKERS = ["kafka-1:9092", "kafka-2:9092"]
INPUT_TOPIC = "orders.raw"

OUTPUT_BROKERS = "kafka-1:9092,kafka-2:9092"
OUTPUT_TOPIC = "orders.normalized"


class RawOrder(msgspec.Struct):
    order_id: str
    user_id: str
    amount: float
    status: str
    channel: str
    event_ts: int


class NormalizedOrder(msgspec.Struct):
    order_id: str
    user_id: str
    amount_cent: int
    paid: bool
    labels: tuple[str, ...]
    event_ts: int


_raw_order_decoder = msgspec.json.Decoder(RawOrder)
_normalized_order_encoder = msgspec.json.Encoder()


SOURCES = {
    "orders-raw-v1": {
        "kind": "kafka",
        "brokers": INPUT_BROKERS,
        "topic": INPUT_TOPIC,
        "initial_offset": "latest",
    }
}


RESOURCES = {
    "order-labels-v1": {
        # 框架内置的高性能等值规则资源。
        # 加载/热更新时会编译成哈希索引，运行时不会逐条扫描所有规则。
        "kind": "eq_rule_labeler",
        # msgspec.Struct 用 attr；如果 records 是 dict，就改成 "dict"。
        "record_mode": "attr",
        # True 表示一条数据可以命中多个标签；False 表示命中第一个就返回。
        "multi_match": True,
        "rules": [
            {
                "when": {"status": "paid"},
                "label": "已支付订单",
            },
            {
                "when": {"status": "paid", "channel": "app"},
                "label": "APP已支付订单",
            },
            {
                "when": {"status": "cancelled"},
                "label": "已取消订单",
            },
        ],
        # 如果规则来自文件，可以改成：
        # "path": "/data/rules/order-labels-v1.json",
        # "refresh_policy": {"type": "ttl", "seconds": 60},
    }
}


HANDLERS = [
    {
        "entrypoint": "normalize_orders_to_kafka",
        "sources": ["orders-raw-v1"],
        "resources": ["order-labels-v1"],
        # backlog >= 1 就触发，每批最多取 50_000 条。
        # 高吞吐场景可按单条大小、下游写入耗时、Ray CPU 调整。
        "batch_size": [1, 50_000],
        "cpus_per_task": 1,
        "max_retries": 2,
        "priority": 0,
    }
]


@lru_cache(maxsize=1)
def _producer() -> Producer:
    """每个 Ray worker 进程内复用一个 Kafka Producer。"""

    return Producer(
        {
            "bootstrap.servers": OUTPUT_BROKERS,
            # demo 默认值偏保守；生产可按吞吐调 linger/batch/compression。
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 5,
        }
    )


def _normalize(order: RawOrder, labels: tuple[str, ...]) -> NormalizedOrder:
    return NormalizedOrder(
        order_id=order.order_id,
        user_id=order.user_id,
        amount_cent=int(round(order.amount * 100)),
        paid=order.status == "paid",
        labels=labels,
        event_ts=order.event_ts,
    )


def normalize_orders_to_kafka(
    request: Any,
    records: list[bytes],
    resources: dict[str, Any],
) -> dict[str, Any]:
    """处理一个 Kafka batch。

    输入：
        records: Kafka ``message.value()`` 列表，每个元素是 JSON bytes。

    输出：
        把转换后的 ``NormalizedOrder`` 编码成 JSON bytes，发送到 OUTPUT_TOPIC。
    """

    producer = _producer()
    labeler = resources["order-labels-v1"]
    produced = 0
    skipped = 0

    for raw in records:
        try:
            order = _raw_order_decoder.decode(raw)
        except msgspec.DecodeError:
            skipped += 1
            continue

        normalized = _normalize(order, labeler.match(order))
        payload = _normalized_order_encoder.encode(normalized)

        # key 用 order_id，方便下游按订单聚合/压缩。
        producer.produce(
            OUTPUT_TOPIC,
            key=normalized.order_id.encode("utf-8"),
            value=payload,
        )
        produced += 1

        # 触发 delivery callback / 内部队列维护；非阻塞。
        producer.poll(0)

    # demo 为了语义简单，在 batch 结束 flush，确保 handler 成功返回前消息已交给 Kafka。
    # 如果你要极致吞吐，可以改为更细的异步确认策略，但要同步考虑 checkpoint 语义。
    producer.flush()

    return {
        "dispatch_id": request.dispatch_id,
        "handler": request.handler_id,
        "input_count": len(records),
        "produced_count": produced,
        "skipped_count": skipped,
        "output_topic": OUTPUT_TOPIC,
    }
