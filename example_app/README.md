# 多 Handler 可运行示例

这个示例包含一个 `workers/orders.py`，其中声明两个处理函数：

- `orders_to_jsonl`：目标格式 JSONL，输出到 `demo_output/jsonl/`；
- `orders_to_csv`：目标格式 CSV，输出到 `demo_output/csv/`。

两个函数加入同一个 `orders-fanout` 共享来源组。每个 offset slice 只调用一次 `fetch_orders`，数据进入
Ray Object Store 后，同一个 ObjectRef 会传给 JSONL 和 CSV Handler。两个函数有各自的并发、重试和
输出地址，但共同推进一个 source checkpoint；任一函数失败都不会提前提交 offset。

安装并运行：

```bash
cd ..
python3 -m pip install -e ".[ray]"
cd example_app
python3 main.py
```

示例优先启动一个 4 CPU 的本地 Ray 集群，模拟 partition 0 上的 `[0, 25)` offsets。fetcher 根据
Dispatcher 分配的范围生成一次订单记录，两个处理函数复用它，并使用各自的 `dispatch_id` 命名文件；
重试会覆盖同名文件，因此示例写入是幂等的。

如果运行环境不允许 Ray 创建进程或监听端口，示例会自动退化到线程后端，以便仍能验证完整 Handler/Dispatcher 流程。生产验证可以设置 `DEMO_REQUIRE_RAY=1`，此时 Ray 启动失败会直接报错而不降级。

生产环境替换点：

- 把 `DemoSourceObserver` 换成生产路径下内置的 `SourceObserver`（去掉 demo 赋值即可）；
- 把 `fetch_orders()` 换成指定 Kafka/Postgres 区间读取；
- 把本地文件输出换成 ClickHouse、Postgres、对象存储等输出连接；
- 示例使用 `demo_state/checkpoints.sqlite3` 与 `demo_state/failures.sqlite3`；
  为保证每次演示都重新处理 25 条，`main.py` 启动时会清理它们。生产代码必须删除这个演示清理步骤并保留数据库文件。
