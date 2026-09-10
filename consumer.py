"""
Order consumer.

- Deserializes Avro order messages from the 'orders' topic
- Maintains a running average of price
- On a processing failure, retries with exponential backoff
- After MAX_RETRIES failed attempts, forwards the message to 'orders-dlq'
  and moves on (so one bad message never blocks the stream)

Usage:
    python consumer.py
"""
import io
import time

import fastavro
from confluent_kafka import Consumer, Producer

SCHEMA_PATH = "order.avsc"
TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"
BOOTSTRAP_SERVERS = "localhost:9092"
MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 1

schema = fastavro.schema.load_schema(SCHEMA_PATH)


def deserialize(raw_bytes, schema):
    buf = io.BytesIO(raw_bytes)
    return fastavro.schemaless_reader(buf, schema)


def serialize(record, schema):
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    return buf.getvalue()


def process_order(order):
    """
    Business validation. Raising here simulates a processing failure
    (e.g. a transient downstream error or bad data) that the retry
    logic below needs to handle.
    """
    if order["price"] < 0:
        raise ValueError(f"Invalid price for order {order['orderId']}: {order['price']}")
    return order["price"]


class RunningAverage:
    def __init__(self):
        self.count = 0
        self.total = 0.0

    def update(self, price):
        self.count += 1
        self.total += price
        return self.total / self.count


def handle_message(order, avg_tracker, dlq_producer):
    attempt = 0
    while True:
        try:
            price = process_order(order)
            avg = avg_tracker.update(price)
            print(f"[OK]    order={order['orderId']:>6} product={order['product']:<6} "
                  f"price={price:>8.2f} running_avg={avg:>8.2f}")
            return
        except Exception as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                print(f"[DLQ]   order={order['orderId']} failed after {MAX_RETRIES} retries "
                      f"({e}) -> sending to {DLQ_TOPIC}")
                dlq_producer.produce(DLQ_TOPIC, value=serialize(order, schema))
                dlq_producer.flush()
                return
            backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            print(f"[RETRY {attempt}/{MAX_RETRIES}] order={order['orderId']} error={e} "
                  f"-> retrying in {backoff}s")
            time.sleep(backoff)


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "order-consumer-group",
        "auto.offset.reset": "earliest",
    })
    dlq_producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    consumer.subscribe([TOPIC])

    avg_tracker = RunningAverage()
    print(f"Consumer started, listening on '{TOPIC}'. Ctrl+C to stop.\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            order = deserialize(msg.value(), schema)
            handle_message(order, avg_tracker, dlq_producer)
    except KeyboardInterrupt:
        print("\nStopping consumer.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
