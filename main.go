package main

import (
	"fmt"
	"strings"
	"sync"
)

// 1. 定义事件类型
type Event struct {
	Key   string // 主键（如k1,k2）
	Value string // 值（简化版可不使用）
	Type  string // "insert"/"update"/"delete"
}

// 2. 水印处理器
type WatermarkProcessor struct {
	mu           sync.Mutex
	activeKeys   map[string]bool // 记录水印窗口内（L和H之间）的活跃主键
	inWindow     bool            // 是否处于水印窗口内
	pendingChunk []string        // 存储当前等待合并的数据库分片数据
	outputBuffer []string        // 输出序列
}

func NewWatermarkProcessor() *WatermarkProcessor {
	return &WatermarkProcessor{
		activeKeys: make(map[string]bool),
	}
}

// 3. 处理变更日志事件
func (wp *WatermarkProcessor) ProcessEvent(event Event) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	switch {
	case event.Key == "L": // 低水印
		wp.inWindow = true
		fmt.Println(">>> 收到低水位 L，进入水印窗口")
	case event.Key == "H": // 高水位
		fmt.Println(">>> 收到高水位 H，开始合并 Chunk 并退出窗口")
		// 算法核心步骤：在高水位到达时，合并 Chunk 数据
		for _, key := range wp.pendingChunk {
			if !wp.activeKeys[key] {
				// 如果 Chunk 中的 key 在窗口内没有新的变更，则合并到输出流
				wp.outputBuffer = append(wp.outputBuffer, key+"(chunk)")
			}
		}
		wp.inWindow = false
		wp.activeKeys = make(map[string]bool)
		wp.pendingChunk = nil
	default: // 普通数据事件
		if wp.inWindow {
			wp.activeKeys[event.Key] = true // 记录窗口内活跃 key，用于过滤 Chunk
		}
		wp.outputBuffer = append(wp.outputBuffer, event.Key)
	}
}

// SetPendingChunk 模拟从数据库读取一个分片（Chunk）
func (wp *WatermarkProcessor) SetPendingChunk(chunk []string) {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	wp.pendingChunk = chunk
	fmt.Printf(">>> 已准备分片数据: %v\n", chunk)
}

// 原来的 ProcessChunk 和 flushChunk 逻辑已集成到 ProcessEvent(H) 中

func main() {
	wp := NewWatermarkProcessor()

	// 模拟变更日志流
	// 论文示意图顺序：k2, k3, k4, k1, L, k3, k1, k1, k3, H, k1, k2, k6
	fullLog := []string{"k2", "k3", "k4", "k1", "L", "k3", "k1", "k1", "k3", "H", "k1", "k2", "k6"}

	for _, key := range fullLog {
		// 模拟在窗口期内（L之后，H之前）异步读取数据库 Chunk
		if key == "k3" && wp.inWindow && len(wp.pendingChunk) == 0 {
			// 模拟步骤 (3): SELECT next chunk FROM table
			chunkData := []string{"k2", "k4", "k5", "k6"}
			wp.SetPendingChunk(chunkData)
		}
		wp.ProcessEvent(Event{Key: key})
	}

	// 打印输出顺序
	fmt.Println("\n最终输出序列 (Output Buffer):")
	fmt.Println(strings.Join(wp.outputBuffer, " -> "))
}
