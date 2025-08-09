# Simple Web Server (simulated)

Demonstrates wiring a simulated HTTP server using built-in nodes. Uses `HttpServerNode.simulate_request()` to inject requests without a real server.

## Run

```bash
python examples/simple_web_server/main.py
```

## What it shows
- Routing with `Router`
- Simple transformation with `MapTransformer`
- Metrics collection with `MetricsCollectorNode`
- Serialization with `SerializationNode`
