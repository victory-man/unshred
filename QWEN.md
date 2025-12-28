# unshred 项目指南

## 项目概述
unshred 是一个实时重构 Solana 交易的 Rust 库。该项目专门用于接收 Solana 网络中的 shreds（数据片段），通过前向纠错（FEC）和并行处理技术重建完整的交易。

### 核心功能
- **实时数据接收**: 通过 UDP 协议接收 Solana shreds
- **并行处理**: 跨 FEC 集合和批处理条目进行并行化处理
- **交易重构**: 将 shreds 重构为完整的 Solana 交易
- **可扩展处理**: 支持用户自定义的交易处理器
- **性能监控**: 可选的 Prometheus 指标收集

### 主要技术栈
- **语言**: Rust (Edition 2021)
- **异步运行时**: Tokio
- **网络**: UDP 套接字接收
- **Solana 生态**: solana-sdk, solana-ledger, solana-entry
- **可选指标**: Prometheus (通过 feature flag 启用)
- **序列化**: bincode, wincode
- **并发处理**: dashmap, moka

## 构建和运行

### 基本构建命令
```bash
# 标准构建
cargo build --release

# 带指标的构建
cargo build --release --features metrics

# 运行测试
cargo test

# 检查代码
cargo check
cargo clippy
```

### 示例程序运行
```bash
# 构建并运行 drift-monitor 示例
cd examples/drift-monitor
cargo build --release
cargo run --release

# 使用 docker-compose 部署完整监控栈
cd examples/drift-monitor
docker-compose up -d
```

## 开发约定

### 代码结构
- `src/lib.rs`: 主要的公共 API 导出
- `src/processor.rs`: 核心的 shred 处理逻辑
- `src/receiver.rs`: UDP 接收器实现
- `src/config.rs`: 配置结构体定义
- `src/types.rs`: 内部数据类型
- `src/metrics.rs`: Prometheus 指标收集 (可选)
- `src/wincode.rs`: 自定义编码实现

### 特性标志
- `default`: 无默认特性
- `metrics`: 启用 Prometheus 指标收集功能

### API 设计模式
- 使用 builder 模式配置 UnshredProcessor
- 实现回调式的 TransactionHandler trait
- 支持可选配置项，提供合理的默认值
- 错误处理使用 anyhow::Result

### 依赖版本兼容性
- 特别注意 Solana 相关依赖的版本对齐
- bincode 固定为 =1.3.3 以保持 Solana 兼容性

## 核心使用模式

### 1. 基本用法
```rust
use unshred::{TransactionHandler, UnshredProcessor};

struct MyHandler;
impl TransactionHandler for MyHandler {
    fn handle_transaction(&self, event: &TransactionEvent) -> Result<()> {
        // 处理每个重构的交易
        Ok(())
    }
}

let processor = UnshredProcessor::builder()
    .handler(MyHandler)
    .bind_address("0.0.0.0:8001")
    .build()?;

processor.run().await
```

### 2. 带指标的用法
```rust
use std::sync::Arc;
use prometheus::Registry;

// 启用 metrics 特性
let registry = Arc::new(Registry::new());
let processor = UnshredProcessor::builder()
    .handler(MyHandler)
    .bind_address("0.0.0.0:8001")
    .metrics_registry(registry)
    .build()?;
```

### 3. 自定义配置
```rust
use unshred::UnshredConfig;

let config = UnshredConfig {
    bind_address: "0.0.0.0:9000".to_string(),
    num_fec_workers: Some(4),
    num_batch_workers: Some(8),
};

let processor = UnshredProcessor::builder()
    .handler(MyHandler)
    .config(config)
    .build()?;
```

## 示例项目

### drift-monitor
完整的生产级示例，展示如何：
- 监听 Drift 协议的清算事件
- 将事件存储到 ClickHouse 数据库
- 使用 Prometheus 收集系统指标
- 通过 Grafana 生成可视化仪表板
- 使用 docker-compose 部署完整监控栈

### 项目结构
```
examples/drift-monitor/
├── src/main.rs          # 主程序入口
├── migrations/          # ClickHouse 数据库迁移
├── monitoring/          # Grafana 仪表板配置
├── docker-compose.yml   # 完整部署配置
└── env.example         # 环境变量示例
```

## 性能优化要点

### 并发配置
- `num_fec_workers`: FEC 解码工作线程数
- `num_batch_workers`: 批处理工作线程数
- 建议根据 CPU 核心数进行调整

### 内存管理
- 使用 DashSet 进行线程安全的高性能数据结构
- 采用 Moka 进行高效内存缓存
- 避免不必要的内存分配，优先使用引用

### 网络优化
- 使用高性能的 socket2 库
- UDP 接收器使用零拷贝技术
- 合理设置接收缓冲区大小

## 监控和指标

### 可用指标
启用 metrics 特性后，可收集以下指标：
- 处理的 shreds 数量
- FEC 解码成功率
- 交易重构性能
- 内存使用情况
- 网络接收速率

### 集成方式
```rust
// 初始化指标系统
let registry = Arc::new(Registry::new());
let processor = UnshredProcessor::builder()
    .metrics_registry(registry)
    .build()?;

// 暴露指标端点
warp::serve(warp::path("metrics").and(warp::get()).map(move || {
    // 返回 Prometheus 格式的指标
})).run(([0, 0, 0, 0], 9090)).await;
```

## 故障排除

### 常见问题
1. **Solana 版本兼容性**: 确保 Solana 相关依赖版本保持一致
2. **UDP 接收权限**: 确保绑定端口有足够权限
3. **内存不足**: 调整工作线程数量或增加系统内存
4. **网络延迟**: 考虑网络拓扑和接收器位置

### 调试技巧
- 使用 `RUST_LOG=debug` 环境变量获取详细日志
- 监控 Prometheus 指标识别性能瓶颈
- 检查 FEC 和批处理队列长度
- 验证 bind_address 配置正确性

## 贡献指南

### 开发环境设置
1. 安装 Rust 1.70+ 
2. 克隆项目并进入目录
3. 运行 `cargo test` 验证环境
4. 建议使用 rustfmt 进行代码格式化

### 代码规范
- 遵循 Rust 官方代码风格
- 使用 cargo clippy 进行代码检查
- 添加适当的文档注释
- 为新功能编写测试

### 提交要求
- 确保所有测试通过
- 代码覆盖率不得降低
- 更新相关文档
- 保持向后兼容性

## Qwen Added Memories
- 不准运行`cargo test`命令
- 不允许执行`cargo build`如果你要检查代码正确性请使用`cargo check`
