"""mihomo_pool - mihomo 多节点独立端口池的 Python 客户端。

两种用法：

1. 常驻异步客户端（推荐，高并发场景）
   --------------------------------------------------
   async with Pool(...) as pool:
       results = await pool.gather([pool.get(u) for u in urls])

   底层是一个长驻的 `mihomo-pool serve` 子进程，通过 stdin/stdout
   收发一行一个 JSON。并发由 Go 侧 worker 池执行，Python 只是投递和收集，
   所以不受 GIL 限制，也不会有 asyncio 协程数量的压力。

2. 单次调用
   --------------------------------------------------
   from mihomo_pool import call
   resp = call("https://example.com")

依赖：仅标准库。
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import shlex
import shutil
import subprocess
import sys
import threading
from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

__version__ = "1.0.0"
PROTOCOL_VERSION = 1

__all__ = [
    "Pool",
    "Response",
    "NodeFilter",
    "PoolError",
    "ProtocolError",
    "NodeError",
    "call",
    "batch",
    "find_binary",
    "ProtocolVersionMismatch",
]


# --------------------------------------------------------------------------- #
# 异常
# --------------------------------------------------------------------------- #


class PoolError(RuntimeError):
    """基础异常。"""


class ProtocolError(PoolError):
    """协议层出错：JSON 不合法、字段缺失、握手失败等。"""


class ProtocolVersionMismatch(PoolError):
    """CLI 协议版本与客户端不兼容。"""


class NodeError(PoolError):
    """请求层失败：所有节点都试过仍失败。"""


# --------------------------------------------------------------------------- #
# 数据结构
# --------------------------------------------------------------------------- #


@dataclass
class Response:
    """一次请求的结果。"""

    ok: bool
    status: int = 0
    body: bytes = b""
    header: dict[str, list[str]] = field(default_factory=dict)
    node: str = ""
    proxy_url: str = ""
    attempts: int = 0
    duration_ms: int = 0
    tag: str = ""
    truncated: bool = False
    error: str = ""

    @property
    def text(self) -> str:
        """按 UTF-8 解码正文，非法字节用替换字符。"""
        return self.body.decode("utf-8", errors="replace")

    def json(self) -> Any:
        """把正文当 JSON 解析。"""
        return json.loads(self.body)

    def header_get(self, name: str, default: str = "") -> str:
        """大小写不敏感地取第一个头值。"""
        low = name.lower()
        for k, v in self.header.items():
            if k.lower() == low:
                return v[0] if v else default
        return default

    def raise_for_status(self) -> "Response":
        """非 2xx 或失败时抛异常。"""
        if not self.ok:
            raise NodeError(self.error or f"请求失败 (status={self.status})")
        if self.status >= 400:
            raise NodeError(f"HTTP {self.status}")
        return self

    def __repr__(self) -> str:
        state = "ok" if self.ok else f"err={self.error!r}"
        return (
            f"<Response {state} status={self.status} node={self.node!r} "
            f"len={len(self.body)} {self.duration_ms}ms>"
        )

    @classmethod
    def from_msg(cls, msg: dict[str, Any]) -> "Response":
        body_b64 = msg.get("body_b64") or ""
        try:
            # validate=True：默认会静默丢弃非 base64 字符，那会把协议错误
            # 变成"正文莫名少几个字节"，必须严格解码
            body = base64.b64decode(body_b64, validate=True) if body_b64 else b""
        except Exception as exc:  # noqa: BLE001
            raise ProtocolError(f"body_b64 解码失败: {exc}") from exc
        return cls(
            ok=bool(msg.get("ok")),
            status=int(msg.get("status") or 0),
            body=body,
            header=msg.get("header") or {},
            node=msg.get("node") or "",
            proxy_url=msg.get("proxy_url") or "",
            attempts=int(msg.get("attempts") or 0),
            duration_ms=int(msg.get("duration_ms") or 0),
            tag=msg.get("tag") or "",
            truncated=bool(msg.get("truncated")),
            error=msg.get("error") or "",
        )


@dataclass
class NodeFilter:
    """节点筛选条件。"""

    groups: Sequence[str] | None = None
    types: Sequence[str] | None = None
    name_prefix: str = ""
    require_port: bool = False
    healthy_only: bool = False

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if self.groups:
            out["groups"] = list(self.groups)
        if self.types:
            out["types"] = list(self.types)
        if self.name_prefix:
            out["name_prefix"] = self.name_prefix
        if self.require_port:
            out["require_port"] = True
        if self.healthy_only:
            out["healthy_only"] = True
        return out


# --------------------------------------------------------------------------- #
# 二进制定位
# --------------------------------------------------------------------------- #


def find_binary(explicit: str | None = None) -> str:
    """按顺序查找 mihomo-pool 可执行文件。

    1. 显式传入的路径
    2. 环境变量 MIHOMO_POOL_BIN
    3. 与当前模块同目录
    4. PATH
    """
    if explicit:
        if not os.path.isfile(explicit):
            raise PoolError(f"指定的二进制不存在: {explicit}")
        return explicit

    env = os.environ.get("MIHOMO_POOL_BIN")
    if env and os.path.isfile(env):
        return env

    here = os.path.dirname(os.path.abspath(__file__))
    name = "mihomo-pool.exe" if os.name == "nt" else "mihomo-pool"
    for cand in (os.path.join(here, name), os.path.join(os.path.dirname(here), name)):
        if os.path.isfile(cand):
            return cand

    found = shutil.which("mihomo-pool")
    if found:
        return found

    raise PoolError(
        "找不到 mihomo-pool 可执行文件。请设置 MIHOMO_POOL_BIN 环境变量，"
        "或把它放到 PATH 里。"
    )


# --------------------------------------------------------------------------- #
# 常驻异步客户端
# --------------------------------------------------------------------------- #


class HelloData(dict):
    """握手消息的降级视图：data 缺失时回落到顶层字段。"""

    def __init__(self, top: dict) -> None:
        super().__init__(base=top)

    def get(self, key: str, default: Any = None) -> Any:  # type: ignore[override]
        if key in self:
            return self[key]
        top = self.get("base") or {}
        return top.get(key, default)


class Pool:
    """常驻 JSONL 服务客户端。

    async with Pool(binary="/usr/local/bin/mihomo-pool") as pool:
        r = await pool.get("https://example.com")

    也可以用同步上下文（内部起一个事件循环线程）：

    with Pool() as pool:
        r = pool.get_sync("https://example.com")
    """

    def __init__(
        self,
        binary: str | None = None,
        *,
        binary_mihomo: str | None = None,
        work_dir: str | None = None,
        socks_port: int | None = None,
        api_port: int | None = None,
        base_port: int | None = None,
        secret: str | None = None,
        allow_lan: bool = False,
        workers: int = 32,
        per_node: int = 4,
        max_inflight: int | None = None,
        strategy: str = "least-inflight",
        subscription: str | None = None,
        nodes_file: str | None = None,
        providers: Sequence[str] | None = None,
        no_kernel: bool = False,
        extra_args: Sequence[str] | None = None,
        startup_timeout: float = 60.0,
        env: dict[str, str] | None = None,
        request_timeout: float = 120.0,
    ) -> None:
        self._binary = find_binary(binary)
        self._opts = {
            "binary_mihomo": binary_mihomo,
            "work_dir": work_dir,
            "socks_port": socks_port,
            "api_port": api_port,
            "base_port": base_port,
            "secret": secret,
            "allow_lan": allow_lan,
        }
        self._workers = workers
        self._per_node = per_node
        self._max_inflight = max_inflight
        self._strategy = strategy
        self._subscription = subscription
        self._nodes_file = nodes_file
        self._providers = list(providers or [])
        self._no_kernel = no_kernel
        self._extra_args = list(extra_args or [])
        self._startup_timeout = startup_timeout
        self._request_timeout = request_timeout
        self._env = env

        self._proc: subprocess.Popen | None = None
        self._hello: dict[str, Any] = {}
        self._pending: dict[str, asyncio.Future] = {}
        self._seq = 0
        self._seq_lock = threading.Lock()
        self._reader_task: asyncio.Task | None = None
        self._write_lock: asyncio.Lock | None = None
        self._stderr_buf: list[str] = []
        self._closed = False

    # ---- 构建命令行 ---------------------------------------------------- #

    def _build_cmd(self) -> list[str]:
        cmd = [self._binary, "serve"]
        o = self._opts
        if o["binary_mihomo"]:
            cmd += ["-bin", o["binary_mihomo"]]
        if o["work_dir"]:
            cmd += ["-dir", o["work_dir"]]
        if o["socks_port"]:
            cmd += ["-socks", str(o["socks_port"])]
        if o["api_port"]:
            cmd += ["-api", str(o["api_port"])]
        if o["base_port"]:
            cmd += ["-base", str(o["base_port"])]
        if o["secret"]:
            cmd += ["-secret", o["secret"]]
        if o["allow_lan"]:
            cmd.append("-allow-lan")

        cmd += ["-workers", str(self._workers)]
        cmd += ["-per-node", str(self._per_node)]
        if self._max_inflight:
            cmd += ["-max-inflight", str(self._max_inflight)]
        cmd += ["-strategy", self._strategy]

        if self._subscription:
            cmd += ["-sub", self._subscription]
        if self._nodes_file:
            cmd += ["-nodes", self._nodes_file]
        for p in self._providers:
            cmd += ["-provider", p]
        if self._no_kernel:
            cmd.append("-no-kernel")
        cmd += self._extra_args
        return cmd

    # ---- 生命周期 ------------------------------------------------------ #

    async def __aenter__(self) -> "Pool":
        await self.start()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.aclose()

    def __enter__(self) -> "Pool":
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, name="mihomo-pool-loop", daemon=True
        )
        self._thread.start()
        self._run_sync(self.start())
        return self

    def __exit__(self, *exc_info: Any) -> None:
        try:
            self._run_sync(self.aclose())
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=5)

    def _run_sync(self, coro: Any) -> Any:
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    async def start(self) -> "Pool":
        """启动子进程并完成握手。"""
        if self._proc is not None:
            return self

        cmd = self._build_cmd()
        env = dict(os.environ)
        if self._env:
            env.update(self._env)

        # 测试桩或 .py 形态的可执行文件：用当前解释器启动，
        # 否则内核遇到没有 exec 权限/无 shebang 支持的环境会 ENOEXEC。
        if cmd[0].endswith(".py"):
            cmd = [sys.executable] + cmd

        try:
            self._proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
                env=env,
            )
        except OSError as exc:
            raise PoolError(f"启动 mihomo-pool 失败: {exc}") from exc

        self._write_lock = asyncio.Lock()
        self._reader_task = asyncio.create_task(self._read_loop())

        # 等 hello
        try:
            hello = await asyncio.wait_for(
                self._wait_hello(), timeout=self._startup_timeout
            )
        except asyncio.TimeoutError as exc:
            await self.aclose()
            raise PoolError(
                f"等待握手超时（{self._startup_timeout}s）: {self._stderr_text()}"
            ) from exc
        except Exception:
            await self.aclose()
            raise

        # 协议信息在 hello.data 里；兼容把字段平铺在顶层的旧实现
        data = hello.get("data") or HelloData(hello)
        proto = int(data.get("protocol") or hello.get("protocol") or 0)
        if proto != PROTOCOL_VERSION:
            await self.aclose()
            raise ProtocolVersionMismatch(
                f"协议版本不匹配: CLI={proto} 客户端={PROTOCOL_VERSION}"
            )
        self._hello = dict(data)
        return self

    async def _wait_hello(self) -> dict[str, Any]:
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending["__hello__"] = fut
        return await fut

    @property
    def hello(self) -> dict[str, Any]:
        """握手信息：协议版本、内核状态、节点数等。"""
        return self._hello

    @property
    def node_count(self) -> int:
        return int(self._hello.get("nodes") or 0)

    @property
    def default_proxy(self) -> str:
        return str(self._hello.get("default_proxy") or "")

    async def aclose(self) -> None:
        """关闭子进程。"""
        if self._closed:
            return
        self._closed = True

        proc = self._proc
        self._proc = None

        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except (asyncio.CancelledError, Exception):
                pass
            self._reader_task = None

        if proc is None:
            return

        # 尽力发一次 close，让 Go 侧优雅收尾
        try:
            if proc.stdin and not proc.stdin.closed:
                proc.stdin.write(b'{"op":"close"}\n')
                proc.stdin.flush()
        except Exception:
            pass

        try:
            proc.stdin.close()  # type: ignore[union-attr]
        except Exception:
            pass

        try:
            await asyncio.wait_for(asyncio.to_thread(proc.wait), timeout=10)
        except asyncio.TimeoutError:
            proc.kill()
            await asyncio.to_thread(proc.wait)

        # 显式关掉三个管道，否则 FileIO 要等 GC 才释放，
        # 长跑进程反复建 Pool 会持续泄漏 fd
        for pipe in (proc.stdout, proc.stderr):
            try:
                if pipe is not None and not pipe.closed:
                    pipe.close()
            except Exception:
                pass

        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(PoolError("连接已关闭"))
        self._pending.clear()

    def close(self) -> None:
        """同步关闭（配合 with 使用）。"""
        if hasattr(self, "_loop") and self._loop.is_running():
            self._run_sync(self.aclose())
        else:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                asyncio.run(self.aclose())
            else:
                raise PoolError("在运行中的事件循环里请用 await pool.aclose()")

    # ---- 读写 ---------------------------------------------------------- #

    async def _read_loop(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        stdout = self._proc.stdout
        loop = asyncio.get_running_loop()

        while True:
            try:
                line = await loop.run_in_executor(None, stdout.readline)
            except (ValueError, OSError):
                break
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                # 协议污染：记到 stderr 缓冲，不打断主循环
                self._stderr_buf.append(f"非 JSON 输出: {line[:200]!r}")
                continue

            kind = msg.get("kind") or ""
            mid = msg.get("id") or ""

            if kind == "hello" or mid == "__hello__":
                fut = self._pending.pop("__hello__", None)
                if fut is not None and not fut.done():
                    fut.set_result(msg)
                continue

            fut = self._pending.pop(mid, None) if mid else None
            if fut is not None and not fut.done():
                fut.set_result(msg)

        # 进程结束：唤醒所有等待者
        for mid, fut in list(self._pending.items()):
            if not fut.done():
                fut.set_exception(
                    PoolError(f"内核进程已退出: {self._stderr_text() or '无 stderr 输出'}")
                )
        self._pending.clear()

    async def _send(self, payload: dict[str, Any]) -> None:
        if self._closed or self._proc is None or self._proc.stdin is None:
            raise PoolError("客户端已关闭")
        data = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")
        assert self._write_lock is not None
        async with self._write_lock:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._write_raw, data)

    def _write_raw(self, data: bytes) -> None:
        assert self._proc is not None and self._proc.stdin is not None
        self._proc.stdin.write(data)
        self._proc.stdin.flush()

    def _next_id(self) -> str:
        with self._seq_lock:
            self._seq += 1
            return str(self._seq)

    def _stderr_text(self) -> str:
        parts = list(self._stderr_buf)
        proc = self._proc
        if proc is not None and proc.stderr is not None:
            try:
                # 只在进程结束或明显出错时读，避免阻塞
                if proc.poll() is not None:
                    raw = proc.stderr.read() or b""
                    if raw:
                        parts.append(raw.decode("utf-8", errors="replace"))
            except Exception:
                pass
        return " | ".join(p for p in parts if p)[:2000]

    # ---- 请求 ---------------------------------------------------------- #

    async def _call(self, payload: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
        mid = self._next_id()
        payload = dict(payload)
        payload["id"] = mid

        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending[mid] = fut

        try:
            await self._send(payload)
        except Exception:
            self._pending.pop(mid, None)
            raise

        t = timeout if timeout is not None else self._request_timeout
        try:
            return await asyncio.wait_for(fut, timeout=t)
        except asyncio.TimeoutError as exc:
            self._pending.pop(mid, None)
            raise PoolError(f"请求超时（{t}s，id={mid}）") from exc

    async def request(
        self,
        url: str,
        *,
        method: str = "GET",
        headers: dict[str, str] | None = None,
        body: str | bytes | None = None,
        json_body: Any = None,
        timeout_ms: int | None = None,
        retries: int | None = None,
        tag: str = "",
        hash_key: str = "",
        allow_direct: bool | None = None,
        skip_body: bool = False,
        max_body_bytes: int | None = None,
        node_filter: NodeFilter | None = None,
        check: bool = False,
    ) -> Response:
        """发起一个请求，返回 Response。"""
        payload: dict[str, Any] = {"op": "request", "url": url, "method": method}

        if headers:
            payload["header"] = headers
        if json_body is not None:
            payload["body_json"] = json_body
        elif isinstance(body, bytes):
            payload["body_b64"] = base64.b64encode(body).decode("ascii")
        elif isinstance(body, str):
            payload["body"] = body
        if timeout_ms is not None:
            payload["timeout_ms"] = int(timeout_ms)
        if retries is not None:
            payload["retries"] = int(retries)
        if tag:
            payload["tag"] = tag
        if hash_key:
            payload["hash_key"] = hash_key
        if allow_direct is not None:
            payload["allow_direct"] = bool(allow_direct)
        if skip_body:
            payload["skip_body"] = True
        if max_body_bytes is not None:
            payload["max_body_bytes"] = int(max_body_bytes)
        if node_filter is not None:
            payload.update(node_filter.to_dict())

        msg = await self._call(payload)
        resp = Response.from_msg(msg)
        if check:
            resp.raise_for_status()
        return resp

    async def get(self, url: str, **kw: Any) -> Response:
        return await self.request(url, method="GET", **kw)

    async def post(self, url: str, **kw: Any) -> Response:
        return await self.request(url, method="POST", **kw)

    async def put(self, url: str, **kw: Any) -> Response:
        return await self.request(url, method="PUT", **kw)

    async def delete(self, url: str, **kw: Any) -> Response:
        return await self.request(url, method="DELETE", **kw)

    async def head(self, url: str, **kw: Any) -> Response:
        kw.setdefault("skip_body", True)
        return await self.request(url, method="HEAD", **kw)

    def get_sync(self, url: str, **kw: Any) -> Response:
        """同步版 get，配合 with Pool() 使用。"""
        return self._run_sync(self.get(url, **kw))

    # ---- 批量 ---------------------------------------------------------- #

    async def gather(
        self,
        coros: Iterable[Any],
        *,
        return_exceptions: bool = False,
    ) -> list[Response]:
        """并发跑一批请求。

        与 asyncio.gather 不同：这里是「先把所有请求投递给 Go 侧」，
        真正的并发度由 Go 的 worker 池控制，Python 侧只是等结果。
        所以即便你塞 10 万个请求，Python 也只会有 10 万个协程但不占线程。
        """
        tasks = [asyncio.ensure_future(c) for c in coros]
        return await asyncio.gather(*tasks, return_exceptions=return_exceptions)

    async def map(
        self,
        urls: Sequence[str],
        *,
        concurrency: int | None = None,
        **kw: Any,
    ) -> list[Response]:
        """对一批 URL 做 GET，可限制 Python 侧同时在飞的协程数。"""
        if concurrency is None or concurrency <= 0:
            concurrency = len(urls) or 1
        sem = asyncio.Semaphore(concurrency)

        async def one(u: str) -> Response:
            async with sem:
                return await self.get(u, **kw)

        return await self.gather([one(u) for u in urls])

    # ---- 控制面 -------------------------------------------------------- #

    async def status(self) -> dict[str, Any]:
        return (await self._call({"op": "status"})).get("data") or {}

    async def nodes(self, node_filter: NodeFilter | None = None) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {"op": "nodes"}
        if node_filter is not None:
            payload["args"] = node_filter.to_dict()
        msg = await self._call(payload)
        return (msg.get("data") or {}).get("nodes") or []

    async def refresh(self) -> dict[str, Any]:
        return (await self._call({"op": "refresh"})).get("data") or {}

    async def health(self, concurrency: int = 16) -> dict[str, Any]:
        return (await self._call(
            {"op": "health", "args": {"concurrency": concurrency}}
        )).get("data") or {}

    async def stats(self) -> dict[str, Any]:
        return (await self._call({"op": "stats"})).get("data") or {}

    async def set_strategy(self, strategy: str) -> dict[str, Any]:
        return (await self._call(
            {"op": "strategy", "args": {"strategy": strategy}}
        )).get("data") or {}

    async def ping(self) -> bool:
        msg = await self._call({"op": "ping"})
        return bool(msg.get("ok"))


# --------------------------------------------------------------------------- #
# 单次调用（不起常驻进程）
# --------------------------------------------------------------------------- #


def call(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: str | bytes | None = None,
    json_body: Any = None,
    timeout_ms: int = 20000,
    retries: int = 1,
    allow_direct: bool = False,
    binary: str | None = None,
    binary_mihomo: str | None = None,
    work_dir: str | None = None,
    socks_port: int | None = None,
    api_port: int | None = None,
    base_port: int | None = None,
    no_kernel: bool = False,
    check: bool = False,
) -> Response:
    """单次请求，起一个短命进程。慢但省心，适合脚本里偶尔用。"""
    exe = find_binary(binary)
    cmd = [exe, "call", "-method", method, "-timeout", str(timeout_ms),
           "-retries", str(retries)]
    if binary_mihomo:
        cmd += ["-bin", binary_mihomo]
    if work_dir:
        cmd += ["-dir", work_dir]
    if socks_port:
        cmd += ["-socks", str(socks_port)]
    if api_port:
        cmd += ["-api", str(api_port)]
    if base_port:
        cmd += ["-base", str(base_port)]
    if no_kernel:
        cmd.append("-no-kernel")
    if allow_direct:
        cmd.append("-allow-direct")
    if headers:
        cmd += ["-header", ";".join(f"{k}: {v}" for k, v in headers.items())]
    if json_body is not None:
        cmd += ["-body", json.dumps(json_body)]
        cmd += ["-header", "Content-Type: application/json"]
    elif isinstance(body, str):
        cmd += ["-body", body]
    elif isinstance(body, bytes):
        raise PoolError("单次 call 不支持 bytes body，请用 Pool.request 或 base64 中转")

    cmd.append(url)

    proc = subprocess.run(cmd, capture_output=True, timeout=timeout_ms / 1000 + 120)
    out = proc.stdout.decode("utf-8", errors="replace").strip()
    if not out:
        raise PoolError(
            f"call 无输出（exit={proc.returncode}）: "
            f"{proc.stderr.decode('utf-8', errors='replace')[:500]}"
        )
    try:
        msg = json.loads(out)
    except json.JSONDecodeError as exc:
        raise ProtocolError(f"call 输出不是 JSON: {out[:300]!r}") from exc
    resp = Response.from_msg(msg)
    if check:
        resp.raise_for_status()
    return resp


def batch(
    requests: Sequence[dict[str, Any]],
    *,
    workers: int = 32,
    per_node: int = 4,
    strategy: str = "least-inflight",
    binary: str | None = None,
    binary_mihomo: str | None = None,
    work_dir: str | None = None,
    no_kernel: bool = False,
    timeout_ms: int = 20000,
) -> list[Response]:
    """批量并发请求，起一个短命进程跑完所有任务。

    requests 里每项是 {"url":..., "method":..., "header":..., ...}。
    """
    exe = find_binary(binary)
    cmd = [exe, "batch", "-workers", str(workers), "-per-node", str(per_node),
           "-strategy", strategy, "-timeout", str(timeout_ms)]
    if binary_mihomo:
        cmd += ["-bin", binary_mihomo]
    if work_dir:
        cmd += ["-dir", work_dir]
    if no_kernel:
        cmd.append("-no-kernel")

    payload = json.dumps(requests, ensure_ascii=False).encode("utf-8")
    proc = subprocess.run(cmd, input=payload, capture_output=True, timeout=timeout_ms / 1000 * len(requests) + 300)
    out = proc.stdout.decode("utf-8", errors="replace").strip()
    if not out:
        raise PoolError(
            f"batch 无输出（exit={proc.returncode}）: "
            f"{proc.stderr.decode('utf-8', errors='replace')[:500]}"
        )
    try:
        arr = json.loads(out)
    except json.JSONDecodeError as exc:
        raise ProtocolError(f"batch 输出不是 JSON: {out[:300]!r}") from exc
    return [Response.from_msg(m) for m in arr]


def _split_cmd(cmd: str) -> list[str]:
    """跨平台切分命令行（Windows 用 posix=False 的近似）。"""
    if os.name == "nt":
        return cmd.split()
    return shlex.split(cmd)
