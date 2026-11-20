"""用一个假的 mihomo-pool 桩验证 Python 客户端的协议实现。"""

from __future__ import annotations

import base64
import io
import json
import os
import stat
import sys
import tempfile
import textwrap
import unittest

# 让测试能直接 import 到包
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from mihomo_pool import (  # noqa: E402
    NodeFilter,
    Pool,
    PoolError,
    ProtocolVersionMismatch,
    Response,
    find_binary,
)

STUB = textwrap.dedent(
    r'''
    #!/usr/bin/env python3
    """假的 mihomo-pool serve：按固定规则回包，用于验证客户端协议。"""
    import base64
    import json
    import sys
    import time

    def out(obj):
        sys.stdout.write(json.dumps(obj, ensure_ascii=False) + "\n")
        sys.stdout.flush()

    # 握手
    out({"kind": "hello", "ok": True, "data": {
        "protocol": 1, "cli": "stub", "lib": "stub",
        "state": "running", "version": "v1.19.34",
        "nodes": 3, "healthy": 3, "ports": 3,
        "default_proxy": "socks5://127.0.0.1:17890",
        "max_inflight": 256,
    }})

    count = 0
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except Exception as e:
            out({"kind": "err", "ok": False, "error": "bad json: %s" % e})
            continue
        mid = msg.get("id", "")
        op = msg.get("op") or "request"

        if op == "close":
            out({"id": mid, "op": "close", "kind": "ack", "ok": True})
            break
        if op == "ping":
            out({"id": mid, "op": "ping", "kind": "ack", "ok": True})
            continue
        if op == "status":
            out({"id": mid, "op": "status", "kind": "ack", "ok": True,
                 "data": {"state": "running", "nodes": 3, "healthy": 3}})
            continue
        if op == "nodes":
            out({"id": mid, "op": "nodes", "kind": "ack", "ok": True,
                 "data": {"nodes": [
                    {"name": "a", "type": "ss", "port": 17900, "healthy": True},
                    {"name": "b", "type": "ss", "port": 17901, "healthy": True},
                 ], "total": 2}})
            continue
        if op == "health":
            out({"id": mid, "op": "health", "kind": "ack", "ok": True,
                 "data": {"healthy": 3, "nodes": 3, "detail": [
                    {"node": "a", "delay_ms": 42, "ok": True, "error": ""}]}})
            continue
        if op == "stats":
            out({"id": mid, "op": "stats", "kind": "ack", "ok": True,
                 "data": {"workers": 32, "queue": 0, "inflight": 0}})
            continue
        if op == "strategy":
            out({"id": mid, "op": "strategy", "kind": "ack", "ok": True,
                 "data": {"strategy": msg.get("args", {}).get("strategy", "")}})
            continue
        if op != "request" and op != "http":
            out({"id": mid, "op": op, "kind": "err", "ok": False,
                 "error": "unknown op"})
            continue

        # 请求：按 url 决定回包
        url = msg.get("url", "")
        count += 1
        if "fail" in url:
            out({"id": mid, "op": "request", "kind": "err", "ok": False,
                 "node": "b", "attempts": 3,
                 "error": "all nodes failed", "duration_ms": 12})
            continue
        if "slow" in url:
            time.sleep(0.05)

        body = ("echo:" + url).encode()
        out({
            "id": mid, "op": "request", "kind": "resp", "ok": True,
            "status": 200,
            "header": {"Content-Type": ["text/plain"], "X-Count": [str(count)]},
            "body_b64": base64.b64encode(body).decode(),
            "body_len": len(body),
            "node": "a", "proxy_url": "socks5://127.0.0.1:17900",
            "attempts": 1, "duration_ms": 7,
            "tag": msg.get("tag", ""),
        })
    '''
)


def make_stub() -> str:
    fd, path = tempfile.mkstemp(prefix="mihomo-pool-stub-", suffix=".py")
    os.close(fd)
    with open(path, "w", encoding="utf-8") as f:
        f.write(STUB)
    os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR | stat.S_IXGRP)
    return path


@unittest.skipIf(os.name == "nt", "桩脚本用 shebang，Windows 下跳过")
class TestPoolAgainstStub(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.stub = make_stub()
        self.pool = Pool(binary=self.stub)
        await self.pool.start()

    async def asyncTearDown(self) -> None:
        await self.pool.aclose()
        try:
            os.unlink(self.stub)
        except OSError:
            pass

    async def test_hello(self) -> None:
        self.assertEqual(self.pool.node_count, 3)
        self.assertEqual(self.pool.default_proxy, "socks5://127.0.0.1:17890")
        self.assertEqual(self.pool.hello.get("version"), "v1.19.34")

    async def test_basic_get(self) -> None:
        r = await self.pool.get("https://example.com/x")
        self.assertTrue(r.ok)
        self.assertEqual(r.status, 200)
        self.assertEqual(r.text, "echo:https://example.com/x")
        self.assertEqual(r.node, "a")
        self.assertEqual(r.duration_ms, 7)
        self.assertEqual(r.header_get("content-type"), "text/plain")

    async def test_response_helpers(self) -> None:
        r = await self.pool.get("https://example.com/x")
        self.assertEqual(r.raise_for_status(), r)
        self.assertIn("ok", repr(r))

    async def test_failure_raises(self) -> None:
        r = await self.pool.get("https://example.com/fail")
        self.assertFalse(r.ok)
        self.assertIn("all nodes failed", r.error)
        self.assertEqual(r.attempts, 3)
        with self.assertRaises(Exception):
            r.raise_for_status()

    async def test_check_kwarg(self) -> None:
        with self.assertRaises(Exception):
            await self.pool.get("https://example.com/fail", check=True)

    async def test_json_body(self) -> None:
        r = await self.pool.post("https://example.com/p", json_body={"a": 1})
        self.assertTrue(r.ok)

    async def test_bytes_body(self) -> None:
        r = await self.pool.post("https://example.com/p", body=b"\x00\xff")
        self.assertTrue(r.ok)

    async def test_tag_passthrough(self) -> None:
        r = await self.pool.get("https://example.com/t", tag="mytag")
        self.assertEqual(r.tag, "mytag")

    async def test_concurrent_gather(self) -> None:
        urls = [f"https://example.com/{i}" for i in range(30)]
        results = await self.pool.gather([self.pool.get(u) for u in urls])
        self.assertEqual(len(results), 30)
        self.assertTrue(all(r.ok for r in results))
        bodies = {r.text for r in results}
        self.assertEqual(len(bodies), 30, "每个请求应拿到独立响应")

    async def test_map_with_concurrency(self) -> None:
        urls = [f"https://example.com/slow/{i}" for i in range(12)]
        results = await self.pool.map(urls, concurrency=4)
        self.assertEqual(len(results), 12)
        self.assertTrue(all(r.ok for r in results))

    async def test_interleaved_ids(self) -> None:
        # 并发时 id 不能串台：每个响应的正文必须对应自己的 url
        urls = [f"https://example.com/id/{i}" for i in range(50)]
        results = await self.pool.gather([self.pool.get(u) for u in urls])
        for u, r in zip(urls, results):
            self.assertEqual(r.text, "echo:" + u)

    async def test_status_op(self) -> None:
        st = await self.pool.status()
        self.assertEqual(st["state"], "running")

    async def test_nodes_op(self) -> None:
        nodes = await self.pool.nodes()
        self.assertEqual(len(nodes), 2)
        self.assertEqual(nodes[0]["name"], "a")

    async def test_nodes_with_filter(self) -> None:
        nodes = await self.pool.nodes(NodeFilter(groups=["hk"]))
        self.assertEqual(len(nodes), 2)

    async def test_health_op(self) -> None:
        h = await self.pool.health(concurrency=8)
        self.assertEqual(h["healthy"], 3)
        self.assertEqual(h["detail"][0]["delay_ms"], 42)

    async def test_stats_op(self) -> None:
        s = await self.pool.stats()
        self.assertEqual(s["workers"], 32)

    async def test_set_strategy(self) -> None:
        d = await self.pool.set_strategy("round-robin")
        self.assertEqual(d["strategy"], "round-robin")

    async def test_ping(self) -> None:
        self.assertTrue(await self.pool.ping())

    async def test_close_then_error(self) -> None:
        await self.pool.aclose()
        with self.assertRaises(PoolError):
            await self.pool.get("https://example.com/x")

    async def test_double_close_safe(self) -> None:
        await self.pool.aclose()
        await self.pool.aclose()  # 不应抛


class TestProtocolVersionMismatch(unittest.IsolatedAsyncioTestCase):
    async def test_rejects_bad_protocol(self) -> None:
        fd, path = tempfile.mkstemp(prefix="stub-bad-", suffix=".py")
        os.close(fd)
        with open(path, "w", encoding="utf-8") as f:
            f.write(
                "#!/usr/bin/env python3\n"
                "import json,sys\n"
                "sys.stdout.write(json.dumps({'kind':'hello','ok':True,"
                "'data':{'protocol':99}})+'\\n')\n"
                "sys.stdout.flush()\n"
            )
        os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR | stat.S_IXGRP)
        try:
            pool = Pool(binary=path)
            with self.assertRaises(ProtocolVersionMismatch):
                await pool.start()
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass


class TestResponseParsing(unittest.TestCase):
    def test_from_msg_minimal(self) -> None:
        r = Response.from_msg({"ok": True, "status": 204})
        self.assertTrue(r.ok)
        self.assertEqual(r.status, 204)
        self.assertEqual(r.body, b"")
        self.assertEqual(r.text, "")

    def test_from_msg_bad_b64(self) -> None:
        with self.assertRaises(Exception):
            Response.from_msg({"ok": True, "body_b64": "!!!!"})

    def test_json_body_decode(self) -> None:
        r = Response.from_msg({
            "ok": True,
            "body_b64": base64.b64encode(b'{"a":1}').decode(),
        })
        self.assertEqual(r.json(), {"a": 1})

    def test_raise_for_status(self) -> None:
        r = Response(ok=True, status=500)
        with self.assertRaises(Exception):
            r.raise_for_status()

    def test_header_get_case_insensitive(self) -> None:
        r = Response(ok=True, header={"X-Test": ["v"]})
        self.assertEqual(r.header_get("x-test"), "v")
        self.assertEqual(r.header_get("missing", "def"), "def")


class TestNodeFilter(unittest.TestCase):
    def test_to_dict_empty(self) -> None:
        self.assertEqual(NodeFilter().to_dict(), {})

    def test_to_dict_full(self) -> None:
        d = NodeFilter(
            groups=["hk"], types=["ss"], name_prefix="p-",
            require_port=True, healthy_only=True,
        ).to_dict()
        self.assertEqual(d["groups"], ["hk"])
        self.assertEqual(d["types"], ["ss"])
        self.assertEqual(d["name_prefix"], "p-")
        self.assertTrue(d["require_port"])
        self.assertTrue(d["healthy_only"])


class TestFindBinary(unittest.TestCase):
    def test_explicit_missing(self) -> None:
        with self.assertRaises(PoolError):
            find_binary("/nonexistent/mihomo-pool")

    def test_explicit_present(self) -> None:
        fd, path = tempfile.mkstemp()
        os.close(fd)
        try:
            self.assertEqual(find_binary(path), path)
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main(verbosity=2)
