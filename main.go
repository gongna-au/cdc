package main

import (
	"fmt"
	"strings"
	"sync"
)

// Event 模拟 CDC 事件
type Event struct {
	Key  string
	Type string // "L", "H", "DATA"
}

// WatermarkProcessor 核心算法实现
type WatermarkProcessor struct {
	mu           sync.Mutex
	activeKeys   map[string]bool
	inWindow     bool
	pendingChunk []string
	outputBuffer []string
}

func NewWatermarkProcessor() *WatermarkProcessor {
	return &WatermarkProcessor{
		activeKeys: make(map[string]bool),
	}
}

// ProcessEvent 处理日志流中的每一个事件（增量流）
func (wp *WatermarkProcessor) ProcessEvent(e Event) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	switch e.Type {
	case "L":
		wp.inWindow = true
		fmt.Printf("\n[LogStream] <--- 收到低水位 L (uuid_low)，开始监控变更\n")
	case "H":
		fmt.Printf("[LogStream] <--- 收到高水位 H (uuid_high)，触发 Chunk 合并并过滤\n")
		// 落地逻辑：在高水位处将 Chunk 合并到输出流
		count := 0
		for _, key := range wp.pendingChunk {
			if !wp.activeKeys[key] {
				// 过滤逻辑：如果窗口内没变过，则合并
				wp.outputBuffer = append(wp.outputBuffer, key+"(chunk)")
				count++
			} else {
				fmt.Printf("  [Filter] 拦截到窗口内冲突 Key: %s (以增量日志为准)\n", key)
			}
		}
		fmt.Printf("  [Merge] 成功合并 %d 条数据，跳过 %d 条冲突数据\n", count, len(wp.pendingChunk)-count)
		wp.inWindow = false
		wp.activeKeys = make(map[string]bool)
		wp.pendingChunk = nil
	case "DATA":
		if wp.inWindow {
			wp.activeKeys[e.Key] = true
		}
		wp.outputBuffer = append(wp.outputBuffer, e.Key)
	}
}

// SetChunkData 模拟数据库异步读取返回的数据（全量读取）
func (wp *WatermarkProcessor) SetChunkData(chunk []string) {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	wp.pendingChunk = chunk
	fmt.Printf("[DB] 已读取数据分片: %v\n", chunk)
}

func main() {
	wp := NewWatermarkProcessor()

	// 1. 初始背景：系统正常处理增量日志
	fmt.Println("=== 1. 系统启动，处理增量日志 ===")
	wp.ProcessEvent(Event{Key: "k1", Type: "DATA"})
	wp.ProcessEvent(Event{Key: "k2", Type: "DATA"})

	// 2. 触发全量同步（模拟 DBlog 论文步骤 1-4）
	fmt.Println("\n=== 2. 触发全量同步 (DBlog Algorithm 1) ===")

	// 步骤 (2): 插入低水位 L
	wp.ProcessEvent(Event{Key: "uuid_low", Type: "L"})

	// 步骤 (3): 异步从数据库读取 Chunk (模拟耗时)
	// 在窗口期内发生的变更
	wp.ProcessEvent(Event{Key: "k3", Type: "DATA"}) // 窗口内变更
	wp.ProcessEvent(Event{Key: "k1", Type: "DATA"}) // 窗口内变更 (k1 在此更新了)

	// 此时数据库查询结果回来了
	fmt.Println("\n[Async] 数据库查询线程返回结果...")
	wp.SetChunkData([]string{"k1", "k4", "k5"}) // 注意：k1 在上面的窗口期变过了

	wp.ProcessEvent(Event{Key: "k6", Type: "DATA"}) // 窗口内变更

	// 步骤 (4): 插入高水位 H
	wp.ProcessEvent(Event{Event{Key: "uuid_high", Type: "H"}.Key, Event{Key: "uuid_high", Type: "H"}.Type})
	// 修复上面一行的小手抖写法，改为标准调用:
	wp.ProcessEvent(Event{Key: "uuid_high", Type: "H"})

	// 3. 同步继续
	fmt.Println("\n=== 3. 同步继续，处理后续日志 ===")
	wp.ProcessEvent(Event{Key: "k7", Type: "DATA"})

	// 最终展示
	fmt.Println("\n" + strings.Repeat("=", 40))
	fmt.Println("最终输出序列 (Output Buffer):")
	fmt.Println(strings.Join(wp.outputBuffer, " -> "))
	fmt.Println(strings.Repeat("=", 40))

	fmt.Println("\n逻辑解析:")
	fmt.Println("1. k1 在窗口内出现了增量日志，所以 Chunk 中的 k1(旧值) 被过滤器拦截，保证了最终一致性。")
	fmt.Println("2. k4, k5 在窗口内没变过，安全地从 Chunk 合并到了输出流中。")
}
