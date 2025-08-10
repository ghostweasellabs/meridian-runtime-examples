from __future__ import annotations

from meridian.core import Message, Node, PortSpec
from meridian.core.ports import Port, PortDirection
from meridian.observability.logging import get_logger, with_context


class Consumer(Node):
    def __init__(self) -> None:
        super().__init__(
            name="consumer",
            inputs=[Port("in", PortDirection.INPUT, spec=PortSpec("in", int))],
            outputs=[],
        )
        self.values: list[int] = []
        self._logger = get_logger()

    def on_stop(self) -> None:
        super().on_stop()
        with with_context(node=self.name):
            self._logger.info(
                "consumer.stop",
                f"Consumer stopping: processed {len(self.values)} messages",
                total=len(self.values),
            )

    def _handle_message(self, port: str, msg: Message[int]) -> None:
        if port != "in":
            return
        self.values.append(msg.payload)
        with with_context(node=self.name, port=port):
            self._logger.info("consumer.received", f"value={msg.payload}")
        print(msg.payload)
