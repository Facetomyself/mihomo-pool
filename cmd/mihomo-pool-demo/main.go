// Command mihomo-pool-demo 演示：从订阅拉节点 → 起内核 → 异步高并发抓取。
//
//	go run ./cmd/mihomo-pool-demo -sub https://sub.example.com/xxx -target https://httpbin.org/get
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	mihomopool "github.com/Facetomyself/mihomo-pool"
)

func main() {
	sub := flag.String("sub", "", "订阅地址（Clash YAML 或分享链接）")
	target := flag.String("target", "https://httpbin.org/get", "目标 URL")
	count := flag.Int("n", 50, "并发请求数")
	workers := flag.Int("workers", 16, "并发 worker 数")
	binary := flag.String("bin", "", "mihomo 可执行文件路径（空则自动查找）")
	workDir := flag.String("dir", "./data/mihomo-pool", "工作目录")
	flag.Parse()

	if *sub == "" {
		fmt.Fprintln(os.Stderr, "必须给 -sub")
		flag.Usage()
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// 1. 拉订阅并解析成节点，这样每个节点能分到独立端口
	fmt.Println("[1/4] 拉取订阅…")
	nodes, err := mihomopool.FetchSubscription(ctx, *sub, mihomopool.FetchOptions{})
	if err != nil {
		log.Fatalf("拉订阅失败: %v", err)
	}
	fmt.Printf("      解析到 %d 个节点\n", len(nodes))
	for i, n := range nodes {
		if i >= 5 {
			fmt.Printf("      … 其余 %d 个\n", len(nodes)-5)
			break
		}
		fmt.Printf("      - %s (%s)\n", n.Name, n.Type)
	}

	// 2. 起内核
	o := mihomopool.DefaultOptions()
	o.Binary = *binary
	o.WorkDir = *workDir
	o.StartTimeout = 30 * time.Second

	pool, err := mihomopool.New(o)
	if err != nil {
		log.Fatalf("初始化失败: %v", err)
	}

	fmt.Println("[2/4] 启动内核…")
	if err := pool.Start(ctx, nodes, nil); err != nil {
		log.Fatalf("启动失败: %v", err)
	}
	defer func() {
		sctx, sc := context.WithTimeout(context.Background(), 15*time.Second)
		defer sc()
		_ = pool.Stop(sctx)
	}()

	st := pool.Status(ctx)
	fmt.Printf("      内核 %s，状态 %s，节点 %d（健康 %d），独立端口 %d\n",
		st.Version, st.State, st.Nodes, st.Healthy, st.Ports)

	// 3. 测速，把慢节点降权
	fmt.Println("[3/4] 并发测速…")
	if h, err := pool.CheckHealth(ctx, 16); err == nil {
		fmt.Printf("      健康 %d/%d\n", h.Healthy, h.Nodes)
	}

	// 4. 并发抓取
	fmt.Printf("[4/4] %d 个请求 / %d 并发…\n", *count, *workers)
	pool.ConfigureExecutor(*workers, *workers)

	reqs := make([]*mihomopool.Request, 0, *count)
	for i := 0; i < *count; i++ {
		reqs = append(reqs, &mihomopool.Request{
			URL:          *target,
			Timeout:      15 * time.Second,
			Retries:      2,
			Tag:          fmt.Sprintf("#%d", i),
			MaxBodyBytes: 256 << 10,
		})
	}

	start := time.Now()
	ch, err := pool.Executor().RunAsync(ctx, reqs)
	if err != nil {
		log.Fatalf("提交失败: %v", err)
	}

	var ok, fail int
	nodeHits := map[string]int{}
	var totalBytes int
	for resp := range ch {
		nodeHits[resp.Node]++
		if resp.OK() {
			ok++
			totalBytes += len(resp.Body)
		} else {
			fail++
			if fail <= 3 {
				fmt.Printf("      失败 %s: %v\n", resp.Tag, resp.Err)
			}
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("\n完成：成功 %d / 失败 %d，耗时 %v（%.1f req/s），下载 %d 字节\n",
		ok, fail, elapsed.Round(time.Millisecond), float64(*count)/elapsed.Seconds(), totalBytes)
	fmt.Println("节点命中分布：")
	for name, c := range nodeHits {
		if name == "" {
			name = "(未分配)"
		}
		fmt.Printf("  %-24s %d\n", name, c)
	}
}
