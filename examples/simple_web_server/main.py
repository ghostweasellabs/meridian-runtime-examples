from __future__ import annotations

import argparse
import asyncio
import json
import queue
import threading
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any
from pathlib import Path

from meridian.core import Message, MessageType, Node, PortSpec, Scheduler, SchedulerConfig, Subgraph
from meridian.core.ports import Port, PortDirection
from meridian.nodes import MapTransformer, Router, Merger

from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
import uvicorn

BASE_DIR = Path(__file__).resolve().parent


def load_welcome_html() -> bytes:
    p = BASE_DIR / "welcome.html"
    try:
        return p.read_text(encoding="utf-8").encode("utf-8")
    except Exception:
        return b"<!doctype html><html><body><h1>Meridian Runtime</h1><p>Welcome.</p></body></html>"

# ---------------------------
# Bridge primitives (thread-safe)
# ---------------------------

@dataclass
class HttpRequest:
    id: str
    method: str
    path: str
    query: dict[str, list[str]]
    ts: float


_request_inbox: "queue.Queue[HttpRequest]" = queue.Queue()
_response_waiters: dict[str, "queue.Queue[tuple[int, bytes, str]]"] = {}
_response_waiters_lock = threading.Lock()


# ---------------------------
# Nodes
# ---------------------------

class RequestSource(Node):
    def __init__(self, name: str = "source") -> None:
        super().__init__(
            name=name,
            inputs=[],
            outputs=[Port("out", PortDirection.OUTPUT, spec=PortSpec("out", dict))],
        )

    def _handle_tick(self) -> None:
        for _ in range(32):
            try:
                req: HttpRequest = _request_inbox.get_nowait()
            except queue.Empty:
                break
            payload = {
                "id": req.id,
                "method": req.method,
                "path": req.path,
                "query": req.query,
                "ts": req.ts,
            }
            self.emit("out", Message(MessageType.DATA, payload))


class ResponseSink(Node):
    def __init__(self, name: str = "response") -> None:
        super().__init__(
            name=name,
            inputs=[Port("in", PortDirection.INPUT, spec=PortSpec("in", dict))],
            outputs=[],
        )
        self.total_requests = 0

    def _handle_message(self, port: str, msg: Message) -> None:
        if port != "in":
            return
        data = msg.payload if isinstance(msg.payload, dict) else {}
        req_id = str(data.get("id", ""))
        status = int(data.get("status", 200))
        body = data.get("body", b"")
        ctype = str(data.get("content_type", "application/json"))
        if isinstance(body, str):
            body = body.encode("utf-8")
        with _response_waiters_lock:
            q = _response_waiters.get(req_id)
        if q is not None:
            q.put((status, body, ctype))
            self.total_requests += 1


def build_graph() -> tuple[Subgraph, ResponseSink]:
    src = RequestSource("source")
    router = Router(
        "router",
        routing_fn=lambda req: "echo" if (isinstance(req, dict) and req.get("path") == "/echo") else "not_found",
        output_ports=["echo", "not_found"],
    )

    def echo_transform(req: dict[str, Any]) -> dict[str, Any]:
        q = req.get("query") or {}
        msg = (q.get("msg") or [""])[0]
        body = json.dumps({"ok": True, "echo": msg, "path": req.get("path")})
        return {"id": req.get("id"), "status": 200, "content_type": "application/json", "body": body}

    echo = MapTransformer("echo", transform_fn=echo_transform)

    def not_found_transform(req: dict[str, Any]) -> dict[str, Any]:
        body = "<html><body><h1>404 Not Found</h1><p>Try /echo?msg=hi</p></body></html>"
        return {"id": req.get("id"), "status": 404, "content_type": "text/html; charset=utf-8", "body": body}

    not_found = MapTransformer("not_found", transform_fn=not_found_transform)

    merge = Merger("merge", input_ports=["echo", "not_found"])  # fan-in to a single stream
    resp = ResponseSink("response")

    g = Subgraph.from_nodes("web", [src, router, echo, not_found, merge, resp])
    g.connect(("source", "out"), ("router", "input"))
    g.connect(("router", "echo"), ("echo", "input"))
    g.connect(("router", "not_found"), ("not_found", "input"))
    g.connect(("echo", "output"), ("merge", "echo"))
    g.connect(("not_found", "output"), ("merge", "not_found"))
    g.connect(("merge", "output"), ("response", "in"))
    return g, resp


# ---------------------------
# FastAPI ingress
# ---------------------------

def create_app(sink: ResponseSink) -> FastAPI:
    app = FastAPI()

    @app.get("/", response_class=HTMLResponse)
    async def index() -> Response:
        return HTMLResponse(load_welcome_html())

    @app.get("/metrics")
    async def metrics() -> Response:
        return JSONResponse({"requests_total": sink.total_requests})

    @app.get("/echo")
    async def echo(request: Request) -> Response:
        # Bridge to graph
        q: "queue.Queue[tuple[int, bytes, str]]" = queue.Queue(maxsize=1)
        req_id = f"req_{int(time.time()*1000)}_{threading.get_ident()}"
        with _response_waiters_lock:
            _response_waiters[req_id] = q
        try:
            query = {k: list(v) for k, v in urllib.parse.parse_qs(str(request.url.query)).items()}
            _request_inbox.put(
                HttpRequest(id=req_id, method="GET", path="/echo", query=query, ts=time.time())
            )
            try:
                status, body, ctype = q.get(timeout=5.0)
            except queue.Empty:
                status, body, ctype = 504, b"Gateway Timeout", "text/plain; charset=utf-8"
        finally:
            with _response_waiters_lock:
                _response_waiters.pop(req_id, None)
        return Response(content=body, status_code=status, media_type=ctype)

    return app


# ---------------------------
# Runner
# ---------------------------

def main() -> None:
    p = argparse.ArgumentParser(description="Meridian FastAPI web server demo")
    p.add_argument("--serve", action="store_true", help="Run a real HTTP server (FastAPI/Uvicorn)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8080)
    args = p.parse_args()

    g, sink = build_graph()
    sched = Scheduler(SchedulerConfig(idle_sleep_ms=1, tick_interval_ms=5, shutdown_timeout_s=3600.0))
    sched.register(g)

    th = threading.Thread(target=sched.run, daemon=True)
    th.start()

    if args.serve:
        app = create_app(sink)
        uvicorn.run(app, host=args.host, port=args.port, log_level="info")
        sched.shutdown(); th.join();
        return

    # Demo mode: simulate a couple of requests via bridge to show graph behavior in tests
    def simulate(path: str) -> tuple[int, bytes, str]:
        req_id = f"demo_{int(time.time()*1000)}"
        waiter: "queue.Queue[tuple[int, bytes, str]]" = queue.Queue(maxsize=1)
        with _response_waiters_lock:
            _response_waiters[req_id] = waiter
        try:
            _request_inbox.put(HttpRequest(id=req_id, method="GET", path=path, query={}, ts=time.time()))
            deadline = time.time() + 3.0
            while True:
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise queue.Empty
                try:
                    return waiter.get(timeout=min(0.2, remaining))
                except queue.Empty:
                    continue
        finally:
            with _response_waiters_lock:
                _response_waiters.pop(req_id, None)

    time.sleep(0.05)
    for path in ["/echo?msg=hi", "/echo?msg=meridian"]:
        status, body, ctype = simulate(path)
        print(f"GET {path} -> {status} {ctype} {len(body)} bytes")

    time.sleep(0.2)
    sched.shutdown(); th.join()


if __name__ == "__main__":
    main()
