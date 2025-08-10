from __future__ import annotations

import threading
import time
from typing import Any

from meridian.core import Scheduler, SchedulerConfig, Subgraph
from meridian.nodes import DataProducer, MapTransformer, MessageQueueNode


def build_graph(use_redis: bool = False) -> Subgraph:
    """Message queue demo: producer -> MQ -> consumer -> sink.

    If use_redis=False, falls back to an in-memory queue so the example
    runs out of the box. Set use_redis=True when a Redis instance is available.
    """
    p = DataProducer("p", data_source=lambda: iter(range(5)), interval_ms=0)

    queue_type = "redis" if use_redis else "inmemory"
    conn: dict[str, Any] = {} if not use_redis else {"host": "127.0.0.1", "port": 6379}

    prod = MessageQueueNode(
        "mq-producer",
        queue_type=queue_type,
        connection_config=conn,
        queue_name="demo",
        mode="producer",
    )
    cons = MessageQueueNode(
        "mq-consumer",
        queue_type=queue_type,
        connection_config=conn,
        queue_name="demo",
        mode="consumer",
    )
    sink = MapTransformer("sink", transform_fn=lambda x: x)

    g = Subgraph.from_nodes("mq", [p, prod, cons, sink])
    g.connect(("p", "output"), ("mq-producer", "input"))
    g.connect(("mq-consumer", "output"), ("sink", "input"))
    return g


def main() -> None:
    print("⚫ MQ pipeline (FastAPI not required):")
    print("   p → mq-producer → [queue] → mq-consumer → sink")
    g = build_graph(use_redis=False)

    s = Scheduler(SchedulerConfig(idle_sleep_ms=0, tick_interval_ms=5, shutdown_timeout_s=4.0))
    s.register(g)
    th = threading.Thread(target=s.run, daemon=True)
    th.start()

    time.sleep(0.3)
    s.shutdown(); th.join()
    print("✓ MQ demo finished (in-memory queue). Switch to Redis by setting use_redis=True in build_graph().")


if __name__ == "__main__":
    main()
