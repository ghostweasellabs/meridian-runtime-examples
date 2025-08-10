from __future__ import annotations

import time
import logging
from typing import Any

from meridian.core import Message, Node, PortSpec
from meridian.core.ports import Port, PortDirection


class SlowSink(Node):
    """Simulates I/O latency and logs processed items; keeps a small buffer."""

    def __init__(self, delay_s: float = 0.02, keep: int = 10) -> None:
        super().__init__(
            name="sink",
            inputs=[
                Port("in", PortDirection.INPUT, spec=PortSpec("in", dict)),
                Port("control", PortDirection.INPUT, spec=PortSpec("control", str)),
            ],
            outputs=[],
        )
        self.delay_s = delay_s
        self.count = 0
        self.processed_items: list[dict[str, Any]] = []
        self.keep = keep
        self.logger = logging.getLogger(f"SlowSink({self.name})")
        self._last_summary = 0.0

    def _handle_message(self, port: str, msg: Message[Any]) -> None:
        if port == "in":
            time.sleep(self.delay_s)
            self.count += 1
            payload = msg.payload if isinstance(msg.payload, dict) else {"_raw": msg.payload}
            self.processed_items.append(payload)
            if len(self.processed_items) > self.keep:
                self.processed_items.pop(0)
            self.logger.info(f"🧺 Sink processed #{self.count}: id={payload.get('id', '?')}")
        elif port == "control":
            cmd = str(msg.payload).lower()
            if cmd == "shutdown":
                self.logger.info("🛑 Received shutdown signal")
                # No special action; scheduler handles shutdown per control-plane policy

    def _handle_tick(self) -> None:
        now = time.monotonic()
        if now - self._last_summary >= 0.5 and self.processed_items:
            self._last_summary = now
            self.logger.info(
                f"📈 Summary: processed={self.count} recent={len(self.processed_items)}"
            )
