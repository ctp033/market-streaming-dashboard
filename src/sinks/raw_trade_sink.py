import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Consumer, KafkaException

BOOTSTRAP_SERVERS = "localhost:9092"
TRADE_TOPIC = "market.trades.raw"

DATA_DIR = "data/raw/trades"

FLUSH_EVERY_N_RECORDS = 50
FLUSH_EVERY_SECONDS = 30

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "raw-trade-sink-group",
    "auto.offset.reset": "earliest",
})


buffers = defaultdict(list)
last_flush_time = time.time()


def parse_iso_timestamp(ts):
    if ts is None:
        return datetime.now(timezone.utc)

    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def get_partition_path(event):
    symbol = event["symbol"]
    event_time = parse_iso_timestamp(event.get("event_timestamp"))
    date_str = event_time.date().isoformat()

    return os.path.join(
        DATA_DIR,
        f"date={date_str}",
        f"symbol={symbol}",
    )


def normalize_trade_event(event, msg):
    """
    Normalizes Finnhub trade events before writing to Parquet.
    Keeps Kafka metadata too, which is useful for debugging/replay.
    """
    event_time = parse_iso_timestamp(event.get("event_timestamp"))
    ingest_time = parse_iso_timestamp(event.get("ingest_timestamp"))

    return {
        "symbol": event.get("symbol"),
        "event_type": event.get("event_type"),
        "price": float(event.get("price")) if event.get("price") is not None else None,
        "size": int(event.get("size")) if event.get("size") is not None else None,
        "conditions": json.dumps(event.get("conditions", [])),
        "event_timestamp": event_time,
        "finnhub_timestamp_ms": event.get("finnhub_timestamp_ms"),
        "ingest_timestamp": ingest_time,
        "source": event.get("source", "finnhub"),

        # Kafka metadata
        "kafka_topic": msg.topic(),
        "kafka_partition": msg.partition(),
        "kafka_offset": msg.offset(),
    }


def write_partition_events(partition_path, records):
    if not records:
        return

    os.makedirs(partition_path, exist_ok=True)

    now = datetime.now(timezone.utc)
    file_name = f"part_{now.strftime('%Y%m%d_%H%M%S_%f')}.parquet"
    file_path = os.path.join(partition_path, file_name)

    df = pd.DataFrame(records)
    df.to_parquet(file_path, index=False)

    print(f"Wrote {len(records)} records to {file_path}")


def flush_buffers():
    global buffers

    total_records = 0

    for partition_path, records in list(buffers.items()):
        if records:
            write_partition_events(partition_path, records)
            total_records += len(records)

    buffers = defaultdict(list)

    if total_records > 0:
        print(f"Flushed {total_records} total records to raw data lake.")


def should_flush():
    total_buffered = sum(len(records) for records in buffers.values())
    time_elapsed = time.time() - last_flush_time

    return (
        total_buffered >= FLUSH_EVERY_N_RECORDS
        or time_elapsed >= FLUSH_EVERY_SECONDS
    )


def handle_message(msg):
    raw_value = msg.value().decode("utf-8")
    event = json.loads(raw_value)

    if event.get("event_type") != "trade":
        return

    if event.get("symbol") is None:
        return

    if event.get("price") is None or event.get("size") is None:
        return

    normalized = normalize_trade_event(event, msg)
    partition_path = get_partition_path(event)

    buffers[partition_path].append(normalized)


def main():
    global last_flush_time

    consumer.subscribe([TRADE_TOPIC])

    print(f"Raw trade sink running.")
    print(f"Consuming from topic: {TRADE_TOPIC}")
    print(f"Writing Parquet files under: {DATA_DIR}")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                if should_flush():
                    flush_buffers()
                    last_flush_time = time.time()
                continue

            if msg.error():
                raise KafkaException(msg.error())

            handle_message(msg)

            if should_flush():
                flush_buffers()
                last_flush_time = time.time()

    except KeyboardInterrupt:
        print("\nStopping raw trade sink... flushing remaining records.")

    finally:
        flush_buffers()
        consumer.close()


if __name__ == "__main__":
    main()