"""示例：同步用法（不需要 asyncio 知识）。

    python3 examples/sync_fetch.py --nodes nodes.json https://httpbin.org/get
"""

from __future__ import annotations

import argparse
import sys

sys.path.insert(0, __file__.rsplit("/examples/", 1)[0])
from mihomo_pool import Pool  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("url")
    ap.add_argument("--nodes", help="节点 JSON 文件")
    ap.add_argument("--binary", help="mihomo-pool 可执行文件")
    ap.add_argument("--mihomo", help="mihomo 内核路径")
    ap.add_argument("--dir", default="./data/mihomo-pool")
    args = ap.parse_args()

    with Pool(
        binary=args.binary,
        binary_mihomo=args.mihomo,
        work_dir=args.dir,
        nodes_file=args.nodes,
    ) as pool:
        print(f"节点 {pool.node_count} 个")
        r = pool.get_sync(args.url, retries=2, check=True)
        print(f"HTTP {r.status}  node={r.node}  {r.duration_ms}ms  {len(r.body)} bytes")
        print(r.text[:500])
    return 0


if __name__ == "__main__":
    sys.exit(main())
