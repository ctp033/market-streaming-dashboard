import json

from confluent_kafka import Consumer, KafkaException

BOOTSTRAP_SERVERS = "localhost:9092"
MOCK_TRADE_TOPIC = "market.trades.mock"

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "mock-trade-consumer-group",
    "auto.offset.reset": "earliest",
})


def main():
    consumer.subscribe([MOCK_TRADE_TOPIC])

    print(f"Listening for mock trades on: {MOCK_TRADE_TOPIC}")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            key = msg.key().decode("utf-8") if msg.key() else None
            event = json.loads(msg.value().decode("utf-8"))

            print("\nMock Finnhub-style trade")
            print("------------------------")
            print(f"Key:       {key}")
            print(f"Topic:     {msg.topic()}")
            print(f"Partition: {msg.partition()}")
            print(f"Offset:    {msg.offset()}")
            print(json.dumps(event, indent=2))

    except KeyboardInterrupt:
        print("\nStopping mock consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()