from __future__ import annotations

from typing import Iterable, Any

from meridian.core import Message, MessageType, Node, PortSpec
from meridian.core.ports import Port, PortDirection


class Feeder(Node):
    """Emits items from a provided iterable as DATA messages on each tick."""

    def __init__(self, items: Iterable[dict[str, Any]], name: str = "feeder") -> None:
        super().__init__(
            name=name,
            inputs=[],
            outputs=[Port("out", PortDirection.OUTPUT, spec=PortSpec("out", dict))],
        )
        self._items = list(items)
        self._idx = 0

    def _handle_tick(self) -> None:
        if self._idx >= len(self._items):
            return
        item = self._items[self._idx]
        self._idx += 1
        self.emit("out", Message(MessageType.DATA, item))
