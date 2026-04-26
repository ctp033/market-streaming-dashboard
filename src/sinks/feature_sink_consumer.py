import json
from datetime import datetime

import psycopg2
from confluent_kafka import Consumer, KafkaException

BOOTSTRAP_SERVERS = "localhost:9092"
FEATURE_TOPIC = "market.features.trade_activity"

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "market_data",
    "user": "market_user",
    "password": "market_password",
}

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "postgres-feature-sink-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})


def parse_timestamp(ts):
    if ts is None:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def connect_postgres():
    return psycopg2.connect(**POSTGRES_CONFIG)


def insert_feature(conn, feature, msg):
    query = """
        INSERT INTO trade_features (
            symbol,
            feature_timestamp,
            last_trade_price,
            trade_count_1m,
            volume_1m,
            dollar_volume_1m,
            rolling_vwap_1m,
            return_1m,
            realized_vol_5m,
            source,
            kafka_topic,
            kafka_partition,
            kafka_offset
        )
        VALUES (
            %(symbol)s,
            %(feature_timestamp)s,
            %(last_trade_price)s,
            %(trade_count_1m)s,
            %(volume_1m)s,
            %(dollar_volume_1m)s,
            %(rolling_vwap_1m)s,
            %(return_1m)s,
            %(realized_vol_5m)s,
            %(source)s,
            %(kafka_topic)s,
            %(kafka_partition)s,
            %(kafka_offset)s
        )
        ON CONFLICT (symbol, feature_timestamp)
        DO NOTHING;
    """

    record = {
        "symbol": feature.get("symbol"),
        "feature_timestamp": parse_timestamp(feature.get("feature_timestamp")),
        "last_trade_price": feature.get("last_trade_price"),
        "trade_count_1m": feature.get("trade_count_1m"),
        "volume_1m": feature.get("volume_1m"),
        "dollar_volume_1m": feature.get("dollar_volume_1m"),
        "rolling_vwap_1m": feature.get("rolling_vwap_1m"),
        "return_1m": feature.get("return_1m"),
        "realized_vol_5m": feature.get("realized_vol_5m"),
        "source": feature.get("source"),
        "kafka_topic": msg.topic(),
        "kafka_partition": msg.partition(),
        "kafka_offset": msg.offset(),
    }

    with conn.cursor() as cur:
        cur.execute(query, record)


def handle_message(conn, msg):
    feature = json.loads(msg.value().decode("utf-8"))

    if feature.get("symbol") is None:
        return

    if feature.get("feature_timestamp") is None:
        return

    insert_feature(conn, feature, msg)


def main():
    conn = connect_postgres()
    consumer.subscribe([FEATURE_TOPIC])

    print("Postgres feature sink running.")
    print(f"Consuming from: {FEATURE_TOPIC}")
    print("Writing to table: trade_features")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            try:
                handle_message(conn, msg)
                conn.commit()
                consumer.commit(message=msg)

                print(
                    f"Inserted feature: topic={msg.topic()} "
                    f"partition={msg.partition()} offset={msg.offset()}"
                )

            except Exception as e:
                conn.rollback()
                print(f"Failed to insert message at offset {msg.offset()}: {e}")

    except KeyboardInterrupt:
        print("\nStopping Postgres feature sink...")

    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()