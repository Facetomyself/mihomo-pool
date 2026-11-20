"""示例：异步高并发抓取。

    python3 examples/async_fetch.py --nodes nodes.json --target https://httpbin.org/get -n 500
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time

sys.path.insert(0, __file__.rsplit("/examples/", 1)[0])
from mihomo_pool import NodeFilter, Pool, PoolError  # noqa: E402


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", help="节点 JSON 文件")
    ap.add_argument("--sub", help="订阅地址")
    ap.add_argument("--target", default="https://httpbin.org/get")
    ap.add_argument("-n", "--count", type=int, default=200)
    ap.add_argument("--workers", type=int, default=64)
    ap.add_argument("--per-node", type=int, default=8)
    ap.add_argument("--concurrency", type=int, default=0, help="Python 侧协程上限，0=不限")
    ap.add_argument("--group", help="只使用某个分组")
    ap.add_argument("--binary", help="mihomo-pool 可执行文件")
    ap.add_argument("--mihomo", help="mihomo 内核路径")
    ap.add_argument("--dir", default="./data/mihomo-pool")
    args = ap.parse_args()

    if not args.nodes and not args.sub:
        ap.error("必须给 --nodes 或 --sub")

    pool = Pool(
        binary=args.binary,
        binary_mihomo=args.mihomo,
        work_dir=args.dir,
        workers=args.workers,
        per_node=args.per_node,
        nodes_file=args.nodes,
        subscription=args.sub,
    )

    try:
        print("启动…", flush=True)
        await pool.start()
        print(
            f"内核 {pool.hello.get('version')}，节点 {pool.node_count} 个，"
            f"默认出口 {pool.default_proxy}",
            flush=True,
        )

        nf = NodeFilter(groups=[args.group]) if args.group else None

        print("测速…", flush=True)
        h = await pool.health(concurrency=16)
        print(f"  健康 {h.get('healthy')}/{h.get('nodes')}", flush=True)

        urls = [f"{args.target}?i={i}" for i in range(args.count)]
        print(f"并发抓取 {args.count} 个（workers={args.workers} per_node={args.per_node}）…", flush=True)

        t0 = time.time()
        if args.concurrency > 0:
            coros = [pool.get(u, retries=2, node_filter=nf) for u in urls]
            results = await pool.gather(coros)
        else:
            results = await pool.map(urls, retries=2, node_filter=nf)
        elapsed = time.time() - t0

        ok = sum(1 for r in results if r.ok)
        node_hits: dict[str, int] = {}
        for r in results:
            node_hits[r.node or "(未分配)"] = node_hits.get(r.node or "(未分配)", 0) + 1

        print(
            f"完成：{ok}/{len(results)} 成功，{elapsed:.2f}s，"
            f"{len(results) / elapsed:.0f} req/s",
            flush=True,
        )
        print("节点分布：")
        for name, c in sorted(node_hits.items(), key=lambda kv: -kv[1]):
            print(f"  {name:<24} {c}")

        fails = [r for r in results if not r.ok]
        if fails:
            print(f"\n前 3 个失败：", flush=True)
            for r in fails[:3]:
                print(f"  {r.tag}: {r.error}", flush=True)

        return 0 if not fails else 1
    except PoolError as exc:
        print(f"失败: {exc}", file=sys.stderr)
        return 1
    finally:
        await pool.aclose()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
