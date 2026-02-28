# DBlog: A Watermark Based Change-Data-Capture Framework 水印算法模拟实现 (Go 版)

本项目是一个基于 Go 语言编写的 DBlog 水印算法（A Watermark Based Change-Data-Capture Framework）的核心逻辑模拟程序。旨在精准复现 Netflix 论文中提出的 DBlog 算法在全量同步（Full Sync）与增量订阅（Incremental Log）并存时的事件合并逻辑。

## 背景引入

在 CDC (Change Data Capture) 场景中，最核心的问题是如何在不停机、不锁表的情况下，实现历史存量数据的平滑迁移，并保证迁移后的数据与增量变更数据最终一致。

DBlog 算法通过**高低水位线**机制，巧妙解决了全量数据分片（Chunk）与增量日志之间的冲突：
- **低水位 (Low Watermark, L)**：标志着 Chunk 开始从数据库读取。
- **高水位 (High Watermark, H)**：标志着 Chunk 读取完成。
- **窗口期 (L to H)**：在此期间发生的所有增量事件都会被记录。如果 Chunk 中的数据也在窗口期内发生了变更，则以增量事件为准，过滤掉 Chunk 中的旧数据。

## 核心算法

本项目模拟了论文中的 **Algorithm 1: Watermarking-based Chunk Selection**：

1.  **暂停日志处理（逻辑概念上）**。
2.  **设置低水位 (L)**：向数据库发送一个特殊标记位。
3.  **获取分片 (Select Chunk)**：从数据库异步读取数据分片。
4.  **设置高水位 (H)**：再次向数据库发送一个特殊标记位。
5.  **恢复日志处理并合并**：
    -   识别 L 和 H 之间的增量事件。
    -   将这些事件放入 `activeKeys` 集合。
    -   **关键点**：当在日志流中检测到 H 事件时，立即合并 Chunk。如果 Key 不在 `activeKeys` 中，则追加到输出缓冲区。

## 运行方法

### 环境要求
- Go 1.18+

### 运行模拟程序
```bash
# 在 cdc 目录下执行
go run main.go
```

### 要求输入序列

```text
k2, k3, k4, k1, L, k3, k1, k1, k3, H, k1, k2, k6
```

- 低水印 (LW) 之前的日志事件。
- 高水印 (HW) 之后的 Chunk 行 (未被日志覆盖的)。
- 高水印 (HW) 之后的日志事件。

### 预期输出序列
```text
k2 -> k3 -> k4 -> k1 -> L -> k3 -> k1 -> k1 -> k3 -> H -> k2(chunk) -> k4(chunk) -> k5(chunk) -> k6(chunk) -> k1 -> k2 -> k6
```
> 注：`k2(chunk)` 表示该数据来自数据库全量读取，其余为增量日志。

## 代码结构说明

- `main.go`: 包含 `WatermarkProcessor` 的定义及其核心业务逻辑。
- `Event`: 模拟 CDC 时间结构。
- `WatermarkProcessor`: 核心处理器，维护 `activeKeys`（活跃主键集合）和 `outputBuffer`（最终输出序列）。

