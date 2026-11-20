package mihomopool

import (
	"fmt"
	"sync"
)

// 配额调度。
//
// 目的：防止某一个订阅吃掉全部并发。
//
// 典型场景——池里有三个订阅：A 便宜但限速低、B 贵但快、C 不稳定。
// 不设配额时，延迟最低的 A 会被打满，B 完全闲置，A 一被限速整体就崩。
// 设了配额（A 最多 50%），调度器会把剩余流量分给 B/C。
//
// 判定口径：以「该订阅在飞数 / 全局在飞数」为占比，
// 与 spec.Quota 比较。全局在飞为 0 时视为不超（首个请求总能通过）。

// quotaTracker 记录每个订阅的在飞数。
type quotaTracker struct {
	mu       sync.Mutex
	inflight map[string]int64
	total    int64
	limits   map[string]float64 // 订阅名 → 最大占比
}

// newQuotaTracker 构造配额跟踪器。
func newQuotaTracker() *quotaTracker {
	return &quotaTracker{
		inflight: make(map[string]int64),
		limits:   make(map[string]float64),
	}
}

// SetLimit 设置订阅的配额上限（0 表示不限）。
func (q *quotaTracker) SetLimit(sub string, ratio float64) {
	if sub == "" {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if ratio <= 0 || ratio >= 1 {
		delete(q.limits, sub)
		return
	}
	q.limits[sub] = ratio
}

// Acquire 尝试为某订阅占一个在飞名额。
//
// 返回 false 表示该订阅已到配额上限，调用方应换一个订阅的节点。
// sub 为空（inline 节点）时不参与配额判定，永远放行。
func (q *quotaTracker) Acquire(sub string) bool {
	if sub == "" {
		q.mu.Lock()
		q.inflight[""]++
		q.total++
		q.mu.Unlock()
		return true
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	limit, has := q.limits[sub]
	if !has || q.total == 0 {
		q.inflight[sub]++
		q.total++
		return true
	}

	// 加这一个之后是否仍在上限内
	after := float64(q.inflight[sub]+1) / float64(q.total+1)
	if after > limit {
		return false
	}
	q.inflight[sub]++
	q.total++
	return true
}

// Release 释放名额。
func (q *quotaTracker) Release(sub string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.inflight[sub] > 0 {
		q.inflight[sub]--
	}
	if q.total > 0 {
		q.total--
	}
}

// Snapshot 返回当前占比，便于观测。
func (q *quotaTracker) Snapshot() map[string]float64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make(map[string]float64, len(q.inflight))
	if q.total == 0 {
		return out
	}
	for sub, n := range q.inflight {
		out[sub] = float64(n) / float64(q.total)
	}
	return out
}

// filterByQuota 从候选里剔除已超配额的订阅节点。
//
// 保留原则：只要还有空闲配额的订阅节点可用就用它们；
// 全部超配额时退回原始候选（宁可略超配额，也不让请求无节点可用）。
func (q *quotaTracker) filterByQuota(candidates []*Node) []*Node {
	if len(candidates) == 0 {
		return candidates
	}

	q.mu.Lock()
	if len(q.limits) == 0 {
		q.mu.Unlock()
		return candidates
	}

	// 先看有哪些订阅已到上限
	blocked := make(map[string]bool, len(q.limits))
	for sub, limit := range q.limits {
		if q.total == 0 {
			continue
		}
		after := float64(q.inflight[sub]+1) / float64(q.total+1)
		if after > limit {
			blocked[sub] = true
		}
	}
	total := q.total
	q.mu.Unlock()

	if len(blocked) == 0 {
		return candidates
	}

	_ = total
	out := make([]*Node, 0, len(candidates))
	for _, n := range candidates {
		if n != nil && blocked[n.Subscription] {
			continue
		}
		out = append(out, n)
	}
	// 全部超配额：宁可略超也不让请求失败
	if len(out) == 0 {
		return candidates
	}
	return out
}

// DescribeQuota 生成人类可读的配额说明。
func (q *quotaTracker) DescribeQuota() string {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.limits) == 0 {
		return "未设置配额"
	}
	if q.total == 0 {
		return fmt.Sprintf("已设 %d 个配额，当前无在飞", len(q.limits))
	}
	s := ""
	for sub, limit := range q.limits {
		cur := float64(q.inflight[sub]) / float64(q.total)
		s += fmt.Sprintf("%s:%.0f%%/%.0f%% ", sub, cur*100, limit*100)
	}
	return s
}
