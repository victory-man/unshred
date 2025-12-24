# Unshred 项目文档

## 项目概述

Unshred 是一个用 Rust 编写的 Solana 事务实时重建系统。该项目通过 UDP 接收 Solana 网络中的 shreds（数据碎片），利用前向纠错（FEC）技术并行处理，重建完整的 Solana 事务，并通过用户提供的处理器进行处理。

### 核心功能
- **UDP 数据接收**：通过 UDP 端口接收 Solana shreds
- **并行处理**：在 FEC 集和批次级别并行化处理
- **事务重建**：将 shreds 重建为完整的 Solana 事务
- **自定义处理器**：通过 `TransactionHandler` trait 允许用户定义事务处理逻辑
- **可选指标**：提供 Prometheus 指标用于系统性能监控

## 技术栈

- **语言**：Rust (Edition 2021)
- **异步运行时**：Tokio
- **Solana 生态**：solana-entry, solana-ledger, solana-sdk
- **并发处理**：crossbeam, dashmap
- **缓存**：moka
- **序列化**：bincode, serde
- **网络**：socket2
- **指标监控**：prometheus (可选)
- **日志**：tracing

## 项目结构

```
unshred/
├── src/
│   ├── lib.rs          # 主要 API 定义和公共接口
│   ├── config.rs       # 配置结构体定义
│   ├── processor.rs    # 核心 shred 处理逻辑
│   ├── receiver.rs     # UDP 接收器
│   ├── types.rs        # 公共类型定义
│   └── metrics.rs      # Prometheus 指标 (可选)
├── docs/               # 文档和展示图表
├── examples/           # 示例项目 (如 drift-monitor)
├── Cargo.toml          # Rust 项目配置
└── README.md          # 项目说明文档
```

## 核心类型和 trait

### TransactionHandler
用户必须实现的核心 trait，用于处理重建的事务：

```rust
pub trait TransactionHandler: Send + Sync + 'static {
    fn handle_transaction(&self, event: &TransactionEvent) -> Result<()>;
}
```

### TransactionEvent
传递给处理器的事件结构：

```rust
pub struct TransactionEvent<'a> {
    pub slot: u64,
    pub transaction: &'a VersionedTransaction,
    pub received_at_micros: Option<u64>,
    pub processed_at_micros: u64,
}
```

### UnshredProcessor
主要的处理器类，使用构建器模式配置：

```rust
let processor = UnshredProcessor::builder()
    .handler(MyHandler)
    .bind_address("0.0.0.0:8001")
    .num_fec_workers(4)
    .num_batch_workers(2)
    .build()?;
```

## 构建和运行

### 基本构建
```bash
cargo build --release
```

### 运行测试
```bash
cargo test
```

### 启用指标功能
```bash
cargo build --features metrics
```

## 配置选项

### UnshredConfig
- `bind_address`: UDP 绑定地址 (默认: "0.0.0.0:8001")
- `num_fec_workers`: FEC 工作线程数 (可选，默认基于 CPU 核心数)
- `num_batch_workers`: 批次工作线程数 (可选)

## 开发约定

### 代码风格
- 使用标准 Rust 格式化：`cargo fmt`
- 使用 Clippy 进行代码检查：`cargo clippy`
- 依赖使用语义版本控制

### 错误处理
- 使用 `anyhow::Result` 作为主要错误类型
- 错误不会停止处理，会记录日志并继续

### 并发模型
- 使用 Tokio 异步运行时
- 跨线程共享数据使用 DashMap 和 Arc
- 工作线程池处理不同类型的任务

## 性能特性

- **内存效率**：使用 Bytes 进行零拷贝字节处理
- **并发处理**：FEC 集和批次级别的并行处理
- **缓存优化**：使用 moka 缓存避免重复处理
- **高性能哈希**：使用 ahash 进行快速哈希计算

## 监控和指标

当启用 `metrics` 特性时，项目提供以下 Prometheus 指标：
- 处理的事务数量
- FEC 恢复统计
- UDP 接收性能指标
- 处理延迟指标

## 示例应用

项目包含 `drift-monitor` 示例，展示如何：
- 过滤 Drift 协议清算事件
- 将事件存储到 ClickHouse
- 使用 Prometheus 跟踪系统性能
- 生成 Grafana 仪表板

## 部署注意事项

- 需要稳定的网络连接以接收 Solana shreds
- 建议在生产环境中启用指标监控
- 在高吞吐量场景下适当调整工作线程数
- 考虑使用 Docker 容器化部署

## 故障排除

- 确保 UDP 端口可访问
- 检查网络延迟和丢包率
- 监控内存使用情况
- 查看处理延迟指标

## 相关资源

- [Solana 文档](https://docs.solana.com/)
- [Tokio 指南](https://tokio.rs/tokio/tutorial)
- [Prometheus 客户端文档](https://docs.rs/prometheus/)