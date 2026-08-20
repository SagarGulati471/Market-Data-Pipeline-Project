# Paper Adapter

# It is a simulator which is designed for internal testing and is intended to mimic the real broker scenarios when orders are placed.

OrderManager.handle_order(signal)
    ├── construct Order (generate order_id = uuid4())
    ├── positions.add_order(order)           ← mark as pending in memory
    ├── paper_adapter.place_order(order)
    │       ├── idempotency check (cache → DB)
    │       ├── _simulate_fill()
    │       ├── _persist_to_db()
    │       └── write to cache
    ├── positions.record_fill(order_id, fill_price)  ← update memory state
    ├── kafka_producer.send(orders_topic, filled_order)
    └── return filled_order