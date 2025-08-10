from __future__ import annotations

import os
import tempfile
import threading
import time

from meridian.core import Message, MessageType, Scheduler, SchedulerConfig, Subgraph
from meridian.nodes import FileReaderNode, FileWriterNode, MapTransformer


def build_graph(tmp_path: str) -> Subgraph:
    """File ingest demo: write a few lines, tail them back out."""
    writer = FileWriterNode("writer", file_path=tmp_path)
    # Identity map to log what is written
    log_map = MapTransformer("logger", transform_fn=lambda x: x)
    reader = FileReaderNode("reader", file_path=tmp_path, polling_interval_ms=10)

    g = Subgraph.from_nodes("file_io", [writer, log_map, reader])
    # Writer and reader operate on the same file; reader emits what it sees.
    return g


def main() -> None:
    print("⚫ File ingest: writer(file) → reader(tail)")
    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, "data.log")
        g = build_graph(path)
        writer = g.nodes["writer"]
        assert isinstance(writer, FileWriterNode)

        s = Scheduler(SchedulerConfig(idle_sleep_ms=0, tick_interval_ms=5, shutdown_timeout_s=4.0))
        s.register(g)
        th = threading.Thread(target=s.run, daemon=True)
        th.start()

        # Append a few lines
        for line in ["a", "b", "c"]:
            writer._handle_message("input", Message(MessageType.DATA, line))
        time.sleep(0.2)
        s.shutdown(); th.join()
    print("✓ File ingest demo finished.")


if __name__ == "__main__":
    main()
