import json
from confluent_kafka import Consumer, KafkaException

TOPIC = "market.trades.raw"

consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": "mock-trade-consumer-group",
        "auto.offset.reset": "earliest",
    }
)


def main():
    consumer.subscribe([TOPIC])
    print("Consumer subscribed. Waiting for messages...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            key = msg.key().decode("utf-8") if msg.key() else None
            value = msg.value().decode("utf-8")
            event = json.loads(value)

            print(
                f"Received: topic={msg.topic()} partition={msg.partition()} "
                f"offset={msg.offset()} key={key} value={event}"
            )

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()