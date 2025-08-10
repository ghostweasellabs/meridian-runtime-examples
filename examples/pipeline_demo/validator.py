from __future__ import annotations
import logging

from typing import Any

from meridian.core import Message, MessageType, Node, PortSpec
from meridian.core.ports import Port, PortDirection


class Validator(Node):
    """Drops invalid inputs, emits valid items only with enhanced logging."""

    def __init__(self) -> None:
        super().__init__(
            name="validator",
            inputs=[Port("in", PortDirection.INPUT, spec=PortSpec("in", dict))],
            outputs=[Port("out", PortDirection.OUTPUT, spec=PortSpec("out", dict))],
        )
        self.seen: int = 0
        self.valid: int = 0
        self.invalid: int = 0
        self.logger = logging.getLogger(f"Validator({self.name})")

    def _handle_message(self, port: str, msg: Message[Any]) -> None:
        """Process incoming messages with detailed validation logging."""
        if port != "in":
            return
            
        self.seen += 1
        payload = msg.payload
        
        self.logger.info(f"🔍 Validating item {self.seen}: {type(payload).__name__}")
        
        # Validate the payload
        if isinstance(payload, dict) and "id" in payload:
            self.valid += 1
            self.logger.info(f"✅ Item {self.seen} VALID: id={payload.get('id')}, value={payload.get('value')}")
            
            # Emit the valid message
            self.emit("out", Message(type=MessageType.DATA, payload=payload))
        else:
            self.invalid += 1
            missing_fields = []
            if not isinstance(payload, dict):
                missing_fields.append("not a dictionary")
            if "id" not in payload:
                missing_fields.append("missing 'id' field")
                
            self.logger.warning(f"❌ Item {self.seen} INVALID: {', '.join(missing_fields)}")
            self.logger.debug(f"   Payload: {payload}")
        
        # Log statistics periodically
        if self.seen % 5 == 0 or self.seen == 1:
            self.logger.info(f"📊 Validation stats: {self.valid}/{self.seen} valid, {self.invalid} invalid")
