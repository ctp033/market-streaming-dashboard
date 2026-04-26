import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "market.trades.raw"

producer = Producer(
    {
        "bootstrap.servers": "localhost:9092",
        "client.id": "mock-trade-producer",
    }
)


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for key={msg.key()}: {err}")
    else:
        print(
            f"Produced to {msg.topic()} [partition {msg.partition()}] @ offset {msg.offset()}"
        )


symbols = ["AAPL", "MSFT", "NVDA", "SPY"]


def make_trade_event():
    symbol = random.choice(symbols)
    price = round(random.uniform(100, 500), 2)
    size = random.choice([10, 25, 50, 100, 200, 500])

    event = {
        "symbol": symbol,
        "event_type": "trade",
        "price": price,
        "size": size,
        "exchange": random.choice([1, 4, 11, 12]),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return event


def main():
    print("Starting mock producer. Press Ctrl+C to stop.")
    try:
        while True:
            event = make_trade_event()
            key = event["symbol"]
            value = json.dumps(event)

            producer.produce(
                TOPIC,
                key=key.encode("utf-8"),
                value=value.encode("utf-8"),
                callback=delivery_report,
            )

            producer.poll(0)
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()