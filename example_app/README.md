# 多 Handler 可运行示例

这个示例包含：

- `workers/orders.py`：旁路 `SOURCES` / `RESOURCES` + `HANDLERS`（扁平声明，调度字段写全）
- `demo_reader.py`：`DemoPayloadReader`，在无真实 Kafka 时按 offset 窗口合成订单

约定摘要：

- **Source**（`demo-orders`）：增量输入，Dispatcher 追进度并 auto-share fetch
- **Resource**（`user-dim-v1`）：`static` 小快照；大维表用 `file`，也可用 `postgres`
  （`query` 或 `table` + `key_column` + `dsn`，在 `preload()` 时加载）
- **output**：透传到 `request.output`；函数内 import，写出只用 `dispatch_id` + `output`

Handler：

- `orders_to_jsonl(request, records, resources)`：用 `user-dim-v1` enrichment 后写 JSONL
- `orders_to_csv(request, records)`：写 CSV

两者声明相同 `sources: ["demo-orders"]`，因此自动共享一次 fetch 与 `shared:demo-orders:0`
checkpoint；各自保留并发、重试与输出地址，任一失败都不会提前提交 offset。

安装并运行：

```bash
cd ..
python3 -m pip install -e ".[ray]"
cd example_app
python3 main.py
```

示例优先启动一个 4 CPU 的本地 Ray 集群，模拟 partition 0 上的 `[0, 25)` offsets。框架通过
`DemoPayloadReader` 按 Dispatcher 分配的范围生成一次订单记录，两个处理函数复用它，并使用各自的
`dispatch_id` 命名文件；重试会覆盖同名文件，因此示例写入是幂等的。

如果运行环境不允许 Ray 创建进程或监听端口，示例会自动退化到线程适配（`LocalThreadAdapter` 同样走
`submit_fetch` + `ResourceLoader`），以便仍能验证完整 Handler/Dispatcher 流程。生产验证可以设置
`DEMO_REQUIRE_RAY=1`，此时 Ray 启动失败会直接报错而不降级。

生产环境替换点：

- 把 `DemoSourceObserver` 换成生产路径下内置的 `SourceObserver`（去掉 demo 赋值即可）；
- 把 `DemoPayloadReader` 换成默认 `CompositePayloadReader`（或自建 Kafka/Postgres reader）；
- 在 worker 模块的 `SOURCES` / `RESOURCES` 中配置真实 brokers / DSN 与声明式资源
  （小配置用 `static`，大维表用 `file` 或 `postgres`）；
- 把本地文件输出换成 ClickHouse、Postgres、对象存储等（路径仍经 `output` 透传）；
- 示例使用 `demo_state/checkpoints.sqlite3` 与 `demo_state/failures.sqlite3`；
  为保证每次演示都重新处理 25 条，`main.py` 启动时会清理它们。生产代码必须删除这个演示清理步骤并保留数据库文件。
