#!/usr/bin/env python3
"""
Minimal Hello World example for Meridian Runtime.

This example:
- Makes a tiny producer send a few numbers
- Passes them through a queue (so bursts don’t snowball)
- Lets a consumer do a little work and print progress

Run with: python -m examples.minimal_hello.main
"""

from __future__ import annotations
import time
import logging

from meridian.core import Message, MessageType, Node, PortSpec, Scheduler, Subgraph
from meridian.core.ports import Port, PortDirection

# Set up logging to see what's happening
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Producer(Node):
    """Produces integer messages on tick intervals with detailed logging."""

    def __init__(self, name: str = "producer", max_count: int = 5):
        super().__init__(
            name=name,
            inputs=[],
            outputs=[Port("output", PortDirection.OUTPUT, spec=PortSpec("output", int))],
        )
        self.max_count = max_count
        self.count = 0
        self.logger = logging.getLogger(f"Producer({name})")

    def _handle_tick(self) -> None:
        """Called on each scheduler tick to produce data."""
        if self.count < self.max_count:
            # Generate some interesting data
            data = self.count * 10 + 5  # Creates: 5, 15, 25, 35, 45
            message = Message(type=MessageType.DATA, payload=data)
            
            self.logger.info(f"🔄 Tick {self.count + 1}: Producing data {data}")
            self.emit("output", message)
            self.count += 1
        else:
            self.logger.info(f"✅ Production complete. Generated {self.max_count} messages.")


class Consumer(Node):
    """Consumes and processes integer messages with detailed logging."""

    def __init__(self, name: str = "consumer"):
        super().__init__(
            name=name,
            inputs=[Port("in", PortDirection.INPUT, spec=PortSpec("in", int))],
            outputs=[],
        )
        self.values = []
        self.logger = logging.getLogger(f"Consumer({name})")
        self.total_processed = 0

    def _handle_message(self, port: str, msg: Message) -> None:
        """Process incoming messages with detailed logging."""
        if port == "in":
            data = msg.payload
            self.values.append(data)
            self.total_processed += 1
            
            # Simulate some processing
            processed_value = data * 2 + 1
            self.logger.info(f"📥 Received: {data} → Processed: {processed_value}")
            
            # Add a small delay to simulate real work
            time.sleep(0.1)
            
            self.logger.info(f"📊 Total processed: {self.total_processed}/{len(self.values)}")


def main():
    """Run the minimal hello world example with enhanced functionality."""
    print("🚀 Tiny dataflow: producer → queue → consumer\n")
    print("• We’ll send 5 numbers, do a bit of work, and measure how long it takes.\n")

    # Create nodes
    producer = Producer(max_count=5)
    consumer = Consumer()

    # Create subgraph and connect nodes
    sg = Subgraph.from_nodes("hello_world", [producer, consumer])
    
    # Connect with a bounded queue to demonstrate backpressure
    sg.connect(("producer", "output"), ("consumer", "in"), capacity=3)
    
    print("🔗 Graph:")
    print(f"   {producer.name} → [capacity=3] → {consumer.name}")
    print()

    # Create scheduler and run
    sch = Scheduler()
    sch.register(sg)

    print("▶️  Starting dataflow...")
    start_time = time.time()
    
    sch.run()
    
    end_time = time.time()
    duration = end_time - start_time

    print(f"\nResults:")
    print(f"   • Produced: {producer.count}")
    print(f"   • Consumed: {len(consumer.values)}")
    print(f"   • Time: {duration:.2f}s  (~{len(consumer.values)/duration:.2f} msg/s)")
    print("✓ Done\n")
    print("Tip: try changing the queue capacity to see how it feels under a burst.")


if __name__ == "__main__":
    main()
