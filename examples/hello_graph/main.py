#!/usr/bin/env python3
"""
Hello Graph example for Meridian Runtime.

This example:
- Emits a few integers
- Routes them through a small queue
- Lets a consumer collect them

Run with: python -m examples.hello_graph.main
"""

from __future__ import annotations
import time
import logging

from meridian.core import Scheduler, Subgraph

from .consumer import Consumer
from .producer import ProducerNode

# Set up logging to see what's happening
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def build_graph(max_count: int = 5) -> tuple[Subgraph, Consumer]:
    """Build a producer-consumer graph with enhanced functionality."""
    logger.info(f"🔨 Building graph with max_count={max_count}")
    
    consumer = Consumer()
    producer = ProducerNode(name="producer", max_count=max_count)
    
    sg = Subgraph.from_nodes("hello_graph", [producer, consumer])

    # Connect producer->consumer with capacity and policy
    # Lower capacity demonstrates backpressure and queue management
    sg.connect(("producer", "output"), ("consumer", "in"), capacity=3)
    
    logger.info(f"🔗 Connected {producer.name}.output → [queue:capacity=3] → {consumer.name}.in")
    
    return sg, consumer


def main() -> None:
    """Run the enhanced hello graph example."""
    print("⚫ Hello graph: producer → queue → consumer\n")
    print("📊 Graph:")
    print("   producer → [capacity=3] → consumer")
    print()

    # Build the graph
    sg, consumer = build_graph(max_count=5)
    
    # Create and run scheduler
    sched = Scheduler()
    sched.register(sg)
    
    print("▶️  Starting dataflow...")
    start_time = time.time()
    
    sched.run()
    
    end_time = time.time()
    duration = end_time - start_time

    # Verify results
    expected_count = 5
    actual_count = len(consumer.values)
    
    print(f"\nResults:")
    print(f"   • Expected: {expected_count}")
    print(f"   • Processed: {actual_count}")
    print(f"   • Time: {duration:.2f}s  (~{actual_count/duration:.2f} msg/s)")
    if actual_count == expected_count:
        print("   • Status: ✓ All good")
    else:
        print(f"   • Status: ! Mismatch")
    
    print(f"\n📈 Data flow summary:")
    print(f"   • Producer generated: {expected_count} messages")
    print(f"   • Consumer processed: {actual_count} messages")
    print(f"   • Queue capacity: 3 (demonstrates backpressure)")
    
    print(f"\n💡 Key takeaways:")
    print(f"   • Messages flow through bounded queues")
    print(f"   • Backpressure prevents memory issues")
    print(f"   • Each node processes independently")
    print(f"   • Scheduler coordinates the entire flow")
    
    # Assert for testing
    assert actual_count == expected_count, f"Expected {expected_count} messages, got {actual_count}"


if __name__ == "__main__":
    main()
