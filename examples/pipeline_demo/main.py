#!/usr/bin/env python3
"""
Pipeline Demo for Meridian Runtime.

This run:
- Generates a small batch of items (some valid, some not)
- Validates → transforms → simulates a slow sink
- Shows simple counts and throughput at the end

Run with: python -m examples.pipeline_demo.main
"""

from __future__ import annotations
import time
import logging
import random

from meridian.core import Scheduler, Subgraph

from .control import KillSwitch
from .feeder import Feeder
from .sink import SlowSink
from .transformer import Transformer
from .validator import Validator

# Set up logging to see what's happening
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def generate_test_data(count: int = 10) -> list[dict]:
    """Generate test data for the pipeline."""
    data = []
    for i in range(count):
        # Generate some valid and invalid data
        if random.random() > 0.3:  # 70% valid data
            item = {
                "id": f"item_{i:03d}",
                "value": random.randint(1, 100),
                "timestamp": time.time(),
                "category": random.choice(["A", "B", "C"])
            }
        else:  # 30% invalid data (missing id)
            item = {
                "value": random.randint(1, 100),
                "timestamp": time.time(),
                "category": random.choice(["A", "B", "C"])
            }
        data.append(item)
    return data


def build_graph() -> tuple[Subgraph, SlowSink, Validator, Transformer]:
    """Build a multi-stage pipeline with enhanced functionality."""
    logger.info("🔨 Building pipeline demo graph")
    
    # Create nodes with enhanced functionality
    validator = Validator()
    transformer = Transformer()
    sink = SlowSink(delay_s=0.02)
    kill_switch = KillSwitch()

    # Feed generated data items
    feeder = Feeder(generate_test_data(15))
    
    sg = Subgraph.from_nodes("pipeline_demo", [feeder, validator, transformer, sink, kill_switch])

    # Wire nodes by port names with appropriate capacities
    # Lower capacities demonstrate backpressure and flow control
    sg.connect(("feeder", "out"), ("validator", "in"), capacity=8)
    sg.connect(("validator", "out"), ("transformer", "in"), capacity=8)
    sg.connect(("transformer", "out"), ("sink", "in"), capacity=4)
    sg.connect(("control", "out"), ("sink", "control"), capacity=1)
    
    logger.info("🔗 Pipeline connections:")
    logger.info("   feeder.out → [queue:8] → validator.in")
    logger.info("   validator.out → [queue:8] → transformer.in")
    logger.info("   transformer.out → [queue:4] → sink.in")
    logger.info("   control.out → [queue:1] → sink.control")
    
    return sg, sink, validator, transformer


def main() -> None:
    """Run the enhanced pipeline demo example."""
    print("⚫ Pipeline: feeder → validator → transformer → sink")
    print("   control → sink (for shutdown/signals)\n")

    # Build the graph
    sg, sink, validator, transformer = build_graph()
    
    print("🏗️  Structure:")
    print("   feeder → validator → transformer → sink")
    print("                 control → sink\n")

    # Create and run scheduler
    sched = Scheduler()
    sched.register(sg)
    
    print("▶️  Running...")
    start_time = time.time()
    
    sched.run()
    
    end_time = time.time()
    duration = end_time - start_time

    # Collect results
    total_valid = validator.valid
    total_seen = validator.seen
    total_invalid = getattr(validator, "invalid", total_seen - total_valid)
    total_processed = len(sink.processed_items)
    
    print(f"\nResults:")
    print(f"   • Items seen by validator: {total_seen}")
    print(f"   • Valid items: {total_valid}")
    print(f"   • Invalid items: {total_invalid}")
    print(f"   • Items processed by sink: {total_processed}")
    print(f"   • Processing time: {duration:.2f} seconds")
    tps = (total_processed / duration) if duration > 0 else 0.0
    print(f"   • Items per second: {tps:.2f}")
    
    print(f"\nTakeaways:")
    print(f"   • Validator filters bad items; transformer tweaks the rest")
    print(f"   • Small queues keep memory flat and throughput steady")
    print(f"   • A slow sink shows how backpressure feels")
    
    # Verify pipeline invariants (do not raise on run; assist tests)
    assert total_seen >= total_valid
    print(f"\n✓ Pipeline complete.")


if __name__ == "__main__":
    main()
