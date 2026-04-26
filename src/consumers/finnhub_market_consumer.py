import json
from confluent_kafka import Consumer, KafkaException

BOOTSTRAP_SERVERS = "localhost:9092"
FEATURE_TOPIC = "market.features.trade_activity"

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "trade-feature-printer-group",
    "auto.offset.reset": "earliest",
})


def format_percent(value):
    if value is None:
        return "N/A"
    return f"{value * 100:.4f}%"


def format_number(value, decimals=4):
    if value is None:
        return "N/A"
    return f"{value:.{decimals}f}"


def print_feature(feature, msg):
    symbol = feature.get("symbol", "UNKNOWN")

    print("\n" + "=" * 60)
    print(f"TRADE ACTIVITY FEATURE | {symbol}")
    print("=" * 60)

    print(f"Topic:      {msg.topic()}")
    print(f"Partition:  {msg.partition()}")
    print(f"Offset:     {msg.offset()}")
    print(f"Timestamp:  {feature.get('feature_timestamp')}")

    print("\nPrice")
    print(f"  Last trade price:   ${format_number(feature.get('last_trade_price'), 4)}")
    print(f"  1-min return:       {format_percent(feature.get('return_1m'))}")
    print(f"  5-min realized vol: {format_percent(feature.get('realized_vol_5m'))}")

    print("\nActivity")
    print(f"  Trade count 1m:     {feature.get('trade_count_1m', 'N/A')}")
    print(f"  Volume 1m:          {feature.get('volume_1m', 'N/A')}")
    print(f"  Dollar volume 1m:   ${format_number(feature.get('dollar_volume_1m'), 2)}")
    print(f"  Rolling VWAP 1m:    ${format_number(feature.get('rolling_vwap_1m'), 4)}")

    print("\nRaw JSON")
    print(json.dumps(feature, indent=2))


def main():
    consumer.subscribe([FEATURE_TOPIC])
    print(f"Listening for trade features on topic: {FEATURE_TOPIC}")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            try:
                feature = json.loads(msg.value().decode("utf-8"))
                print_feature(feature, msg)

            except json.JSONDecodeError:
                print(f"Could not decode message at offset {msg.offset()}: {msg.value()}")

    except KeyboardInterrupt:
        print("\nStopping trade feature consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()