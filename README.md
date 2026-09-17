# Kafka Order Processing — Setup & Demo

## 1. Start Kafka
```
docker-compose up -d
```
Check with:
```
docker logs kafka --tail 20
```

## 2. Install dependencies
```
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## 3. Run the demo

**Terminal 1 — consumer (running average + retry + DLQ):**
```
python consumer.py
```

**Terminal 2 — DLQ monitor (proves failed messages land there):**
```
python dlq_monitor.py
```

**Terminal 3 — producer:**
```
# Normal run — watch the running average update in Terminal 1
python producer.py --count 10 --interval 1

# Retry + DLQ demo — order #5 has an invalid price on purpose
python producer.py --count 8 --interval 1 --bad-order 5
```
In Terminal 1 you'll see 3 retries with growing backoff (1s, 2s, 4s) for
that order, then it gets forwarded to the DLQ. Terminal 2 will print it
as soon as it lands.

- **Avro serialization** — `order.avsc`, read via `fastavro`, used by both
  producer and consumer for encode/decode.
- **Real-time aggregation** — `RunningAverage` in `consumer.py` updates a
  running mean of `price` on every successfully processed message.
- **Retry logic** — `handle_message()` retries a failed order up to
  `MAX_RETRIES` times with exponential backoff before giving up.
- **DLQ** — after retries are exhausted, the original message is
  re-serialized and produced to `orders-dlq` instead of being dropped.
