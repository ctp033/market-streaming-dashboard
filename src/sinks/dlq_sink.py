import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Consumer, KafkaException

BOOTSTRAP_SERVERS = "localhost:9092"
DLQ_TOPIC = "market.errors.dlq"

DATA_DIR = "data/errors/dlq"

FLUSH_EVERY_N_RECORDS = 25
FLUSH_EVERY_SECONDS = 30

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "dlq-sink-group",
    "auto.offset.reset": "earliest",
})


buffers = defaultdict(list)
last_flush_time = time.time()


def now_utc():
    return datetime.now(timezone.utc)


def parse_iso_timestamp(ts):
    if ts is None:
        return now_utc()

    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return now_utc()


def safe_json_loads(value):
    try:
        return json.loads(value)
    except Exception:
        return None


def get_partition_path(record):
    failed_at = record.get("failed_at")
    failed_time = parse_iso_timestamp(failed_at)
    date_str = failed_time.date().isoformat()

    source_topic = record.get("source_topic") or record.get("topic") or "unknown_topic"
    source_topic = source_topic.replace(".", "_")

    return os.path.join(
        DATA_DIR,
        f"date={date_str}",
        f"source_topic={source_topic}",
    )


def normalize_dlq_event(event, msg):
    """
    Normalize DLQ records for durable Parquet storage.

    Expected DLQ event shape from feature engine:
    {
      "error": "...",
      "raw_value": "...",
      "topic": "market.trades.raw",
      "partition": 0,
      "offset": 123,
      "failed_at": "..."
    }
    """

    raw_value = event.get("raw_value")
    raw_json = safe_json_loads(raw_value) if isinstance(raw_value, str) else None

    failed_time = parse_iso_timestamp(event.get("failed_at"))

    return {
        "error": event.get("error"),
        "raw_value": raw_value,
        "raw_value_json": json.dumps(raw_json) if raw_json is not None else None,

        "source_topic": event.get("source_topic") or event.get("topic"),
        "source_partition": event.get("source_partition") or event.get("partition"),
        "source_offset": event.get("source_offset") or event.get("offset"),

        "failed_at": failed_time,

        # Useful fields extracted from original raw event if possible
        "raw_symbol": raw_json.get("symbol") if isinstance(raw_json, dict) else None,
        "raw_event_type": raw_json.get("event_type") if isinstance(raw_json, dict) else None,
        "raw_source": raw_json.get("source") if isinstance(raw_json, dict) else None,

        # Kafka metadata for the DLQ message itself
        "dlq_topic": msg.topic(),
        "dlq_partition": msg.partition(),
        "dlq_offset": msg.offset(),
        "dlq_written_at": now_utc(),
    }


def write_partition_events(partition_path, records):
    if not records:
        return

    os.makedirs(partition_path, exist_ok=True)

    now = now_utc()
    file_name = f"part_{now.strftime('%Y%m%d_%H%M%S_%f')}.parquet"
    file_path = os.path.join(partition_path, file_name)

    df = pd.DataFrame(records)
    df.to_parquet(file_path, index=False)

    print(f"Wrote {len(records)} DLQ records to {file_path}")


def flush_buffers():
    global buffers

    total_records = 0

    for partition_path, records in list(buffers.items()):
        if records:
            write_partition_events(partition_path, records)
            total_records += len(records)

    buffers = defaultdict(list)

    if total_records > 0:
        print(f"Flushed {total_records} DLQ records to error data lake.")


def should_flush():
    total_buffered = sum(len(records) for records in buffers.values())
    time_elapsed = time.time() - last_flush_time

    return (
        total_buffered >= FLUSH_EVERY_N_RECORDS
        or time_elapsed >= FLUSH_EVERY_SECONDS
    )


def handle_message(msg):
    raw_value = msg.value().decode("utf-8", errors="replace")

    try:
        event = json.loads(raw_value)
    except json.JSONDecodeError:
        event = {
            "error": "DLQ message itself was not valid JSON",
            "raw_value": raw_value,
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "failed_at": now_utc().isoformat(),
        }

    normalized = normalize_dlq_event(event, msg)
    partition_path = get_partition_path(normalized)

    buffers[partition_path].append(normalized)


def main():
    global last_flush_time

    consumer.subscribe([DLQ_TOPIC])

    print("DLQ sink running.")
    print(f"Consuming from topic: {DLQ_TOPIC}")
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
        print("\nStopping DLQ sink... flushing remaining records.")

    finally:
        flush_buffers()
        consumer.close()


if __name__ == "__main__":
    main()