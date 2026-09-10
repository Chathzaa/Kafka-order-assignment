"""
Prints messages that land in the DLQ, so it's visible during the demo
that permanently failed orders actually end up there.

Usage:
    python dlq_monitor.py
"""
import io
import fastavro
from confluent_kafka import Consumer

SCHEMA_PATH = "order.avsc"
DLQ_TOPIC = "orders-dlq"
BOOTSTRAP_SERVERS = "localhost:9092"

schema = fastavro.schema.load_schema(SCHEMA_PATH)


def deserialize(raw_bytes, schema):
    buf = io.BytesIO(raw_bytes)
    return fastavro.schemaless_reader(buf, schema)


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "dlq-monitor-group",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([DLQ_TOPIC])
    print(f"Watching '{DLQ_TOPIC}'. Ctrl+C to stop.\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue
            order = deserialize(msg.value(), schema)
            print(f"[DLQ MESSAGE] {order}")
    except KeyboardInterrupt:
        print("\nStopping.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
