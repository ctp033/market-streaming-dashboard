import argparse
import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
REPLAY_TOPIC = "market.trades.replay"
DEFAULT_DATA_PATH = "data/raw/trades/**/*.parquet"

producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "client.id": "replay-trade-producer",
})


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Replay delivery failed: {err}")
    else:
        print(
            f"[INFO] Replayed to {msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def timestamp_to_iso(value):
    if pd.isna(value):
        return datetime.now(timezone.utc).isoformat()

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().astimezone(timezone.utc).isoformat()

    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()

    return str(value)


def normalize_replay_row(row):
    """
    Converts a Parquet row back into the same trade schema
    your feature engine expects.
    """
    return {
        "symbol": row["symbol"],
        "event_type": "trade",
        "price": float(row["price"]),
        "size": int(row["size"]),
        "conditions": json.loads(row["conditions"]) if isinstance(row.get("conditions"), str) else [],
        "event_timestamp": timestamp_to_iso(row["event_timestamp"]),
        "finnhub_timestamp_ms": int(row["finnhub_timestamp_ms"]) if not pd.isna(row.get("finnhub_timestamp_ms")) else None,
        "ingest_timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "replay",
        "original_source": row.get("source", "finnhub"),
    }


def send_trade(event):
    symbol = event["symbol"]

    producer.produce(
        REPLAY_TOPIC,
        key=symbol.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)


def load_replay_data(path, symbol=None, start_time=None, end_time=None):
    df = pd.read_parquet(path)

    if symbol:
        df = df[df["symbol"] == symbol]

    if start_time:
        start_dt = pd.to_datetime(start_time, utc=True)
        df = df[pd.to_datetime(df["event_timestamp"], utc=True) >= start_dt]

    if end_time:
        end_dt = pd.to_datetime(end_time, utc=True)
        df = df[pd.to_datetime(df["event_timestamp"], utc=True) <= end_dt]

    df = df.sort_values("event_timestamp")

    return df


def main():
    parser = argparse.ArgumentParser(description="Replay saved trade Parquet data into Kafka.")
    parser.add_argument("--path", default=DEFAULT_DATA_PATH, help="Parquet path or glob.")
    parser.add_argument("--symbol", default=None, help="Optional symbol filter, e.g. NVDA.")
    parser.add_argument("--start-time", default=None, help="Optional ISO start time.")
    parser.add_argument("--end-time", default=None, help="Optional ISO end time.")
    parser.add_argument("--speed", type=float, default=0.05, help="Sleep time between replayed events.")
    parser.add_argument("--limit", type=int, default=None, help="Optional max number of rows to replay.")

    args = parser.parse_args()

    print("[INFO] Loading replay data...")
    df = load_replay_data(
        path=args.path,
        symbol=args.symbol,
        start_time=args.start_time,
        end_time=args.end_time,
    )

    if args.limit:
        df = df.head(args.limit)

    print(f"[INFO] Replaying {len(df)} trades to topic: {REPLAY_TOPIC}")

    try:
        for _, row in df.iterrows():
            event = normalize_replay_row(row)
            send_trade(event)
            print(f"[INFO] Replayed trade: {event}")
            time.sleep(args.speed)

    except KeyboardInterrupt:
        print("\n[INFO] Stopping replay producer...")

    finally:
        producer.flush()
        print("[INFO] Replay complete.")


if __name__ == "__main__":
    main()