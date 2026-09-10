"""
Order producer.

Publishes randomized order messages to the 'orders' Kafka topic,
serialized against order.avsc.

Usage:
    python producer.py --count 10 --interval 1
    python producer.py --count 10 --interval 1 --bad-order 5
        (sends a message with an invalid negative price as the 5th
         message, to trigger the consumer's retry/DLQ path live)
"""
import argparse
import io
import random
import time

import fastavro
from confluent_kafka import Producer

SCHEMA_PATH = "order.avsc"
TOPIC = "orders"
BOOTSTRAP_SERVERS = "localhost:9092"
PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]

schema = fastavro.schema.load_schema(SCHEMA_PATH)


def serialize(record, schema):
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    return buf.getvalue()


def make_order(order_id, force_bad=False):
    price = round(random.uniform(5.0, 500.0), 2)
    if force_bad:
        # Deliberately invalid -> consumer's process_order() will reject it,
        # forcing the retry -> DLQ path.
        price = -1.0
    return {
        "orderId": str(order_id),
        "product": random.choice(PRODUCTS),
        "price": price,
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for order: {err}")
    else:
        print(f"Delivered to {msg.topic()} [partition {msg.partition()}]")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=10, help="Number of orders to send")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between orders")
    parser.add_argument("--bad-order", type=int, default=None,
                         help="1-based index of a deliberately invalid order (demo retry/DLQ)")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    for i in range(1, args.count + 1):
        order_id = 1000 + i
        force_bad = (args.bad_order == i)
        order = make_order(order_id, force_bad=force_bad)
        payload = serialize(order, schema)

        producer.produce(TOPIC, value=payload, callback=delivery_report)
        producer.poll(0)

        tag = " <-- INVALID (demo)" if force_bad else ""
        print(f"Sent: {order}{tag}")

        time.sleep(args.interval)

    producer.flush()
    print("Done.")


if __name__ == "__main__":
    main()
