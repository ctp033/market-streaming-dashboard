import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
MOCK_TRADE_TOPIC = "market.trades.mock"

producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "client.id": "mock-finnhub-trade-producer",
})

symbols = {
    "AAPL": 170.00,
    "MSFT": 410.00,
    "NVDA": 208.00,
    "SPY": 520.00,
}


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def delivery_report(err, msg):
    if err is not None:
        print(f"Kafka delivery failed: {err}")


def update_price(symbol):
    old_price = symbols[symbol]
    pct_move = random.uniform(-0.001, 0.001)
    new_price = round(old_price * (1 + pct_move), 2)
    symbols[symbol] = new_price
    return new_price


def create_mock_finnhub_trade(symbol):
    price = update_price(symbol)
    size = random.choice([1, 5, 10, 25, 50, 100, 200, 500, 1000])

    event_time = datetime.now(timezone.utc)
    event_timestamp_ms = int(event_time.timestamp() * 1000)

    # Same normalized format your Finnhub producer sends to Kafka
    return {
        "symbol": symbol,
        "event_type": "trade",
        "price": price,
        "size": size,
        "conditions": random.choice([["1"], ["1", "24"], ["12"], []]),
        "event_timestamp": event_time.isoformat(),
        "finnhub_timestamp_ms": event_timestamp_ms,
        "ingest_timestamp": now_iso(),
        "source": "mock_finnhub",
    }


def send_trade(event):
    symbol = event["symbol"]

    producer.produce(
        MOCK_TRADE_TOPIC,
        key=symbol.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )

    producer.poll(0)


def main():
    print(f"Producing mock Finnhub-style trades to: {MOCK_TRADE_TOPIC}")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            symbol = random.choice(list(symbols.keys()))
            event = create_mock_finnhub_trade(symbol)

            send_trade(event)
            print(f"Mock trade -> Kafka: {event}")

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping mock producer...")

    finally:
        producer.flush()


if __name__ == "__main__":
    main()