import json
import html
import time
from collections import Counter, deque
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.express as px
import streamlit as st
from confluent_kafka import Consumer, KafkaException, KafkaError, OFFSET_END, TopicPartition

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    st_autorefresh = None


BOOTSTRAP_SERVERS = "localhost:9092"

RAW_TRADE_TOPIC = "market.trades.raw"
MOCK_TRADE_TOPIC = "market.trades.mock"
FEATURE_TOPIC = "market.features.trade_activity"
DLQ_TOPIC = "market.errors.dlq"

RAW_BUFFER_LIMIT = 1_000
FEATURE_BUFFER_LIMIT = 1_000
DLQ_BUFFER_LIMIT = 200
TELEMETRY_BUFFER_LIMIT = 5_000
POLL_BATCH_SIZE = 250
POLL_TIMEOUT_SECONDS = 0.05
FLOWING_WINDOW_SECONDS = 15
THROUGHPUT_WINDOW_SECONDS = 60
MARKET_TIMEZONE = ZoneInfo("America/New_York")
MARKET_OPEN_TIME = datetime_time(9, 30)
MARKET_CLOSE_TIME = datetime_time(16, 0)

DATA_SOURCE_TOPICS = {
    "Real pipeline": [RAW_TRADE_TOPIC],
    "Mock pipeline": [MOCK_TRADE_TOPIC],
    "Both": [RAW_TRADE_TOPIC, MOCK_TRADE_TOPIC],
}

RAW_TRADE_COLUMNS = [
    "event_timestamp",
    "ingest_timestamp",
    "symbol",
    "price",
    "size",
    "conditions",
    "source",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
]

FEATURE_COLUMNS = [
    "feature_timestamp",
    "symbol",
    "last_trade_price",
    "trade_count_1m",
    "volume_1m",
    "dollar_volume_1m",
    "rolling_vwap_1m",
    "return_1m",
    "realized_vol_5m",
    "source",
]

FEATURE_NUMERIC_COLUMNS = [
    "last_trade_price",
    "trade_count_1m",
    "volume_1m",
    "dollar_volume_1m",
    "rolling_vwap_1m",
    "return_1m",
    "realized_vol_5m",
]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_timestamp(value: Any) -> datetime | None:
    if value is None or pd.isna(value):
        return None

    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def format_number(value: Any, decimals: int = 2, prefix: str = "") -> str:
    if value is None or pd.isna(value):
        return "N/A"

    try:
        return f"{prefix}{float(value):,.{decimals}f}"
    except (TypeError, ValueError):
        return "N/A"


def format_percent(value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"

    try:
        return f"{float(value) * 100:,.3f}%"
    except (TypeError, ValueError):
        return "N/A"


def format_duration(delta: timedelta) -> str:
    total_seconds = max(0, int(delta.total_seconds()))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if hours > 0:
        return f"{hours}h {minutes}m"

    if minutes > 0:
        return f"{minutes}m {seconds}s"

    return f"{seconds}s"


def next_weekday(day: date) -> date:
    next_day = day + timedelta(days=1)
    while next_day.weekday() >= 5:
        next_day += timedelta(days=1)
    return next_day


def get_market_session_state() -> dict[str, str]:
    current_time = datetime.now(MARKET_TIMEZONE)
    current_date = current_time.date()
    open_at = datetime.combine(current_date, MARKET_OPEN_TIME, tzinfo=MARKET_TIMEZONE)
    close_at = datetime.combine(current_date, MARKET_CLOSE_TIME, tzinfo=MARKET_TIMEZONE)
    is_weekday = current_time.weekday() < 5

    if is_weekday and open_at <= current_time < close_at:
        return {
            "status": "Open",
            "detail": f"Closes in {format_duration(close_at - current_time)}",
            "session_time": current_time.strftime("%I:%M:%S %p ET"),
            "next_event": close_at.strftime("%I:%M %p ET"),
        }

    if is_weekday and current_time < open_at:
        return {
            "status": "Pre-market",
            "detail": f"Opens in {format_duration(open_at - current_time)}",
            "session_time": current_time.strftime("%I:%M:%S %p ET"),
            "next_event": open_at.strftime("%I:%M %p ET"),
        }

    next_session_date = next_weekday(current_date)
    next_open = datetime.combine(next_session_date, MARKET_OPEN_TIME, tzinfo=MARKET_TIMEZONE)

    return {
        "status": "Closed",
        "detail": f"Opens in {format_duration(next_open - current_time)}",
        "session_time": current_time.strftime("%I:%M:%S %p ET"),
        "next_event": next_open.strftime("%a %I:%M %p ET"),
    }


def create_consumer(group_id: str, topics: list[str]) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            "client.id": group_id,
            "session.timeout.ms": 10_000,
        }
    )

    metadata = consumer.list_topics(timeout=5)
    assignments = []

    for topic in topics:
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            raise RuntimeError(f"Kafka topic is not available yet: {topic}")

        for partition in topic_metadata.partitions:
            assignments.append(TopicPartition(topic, partition, OFFSET_END))

    consumer.assign(assignments)
    return consumer


def normalize_kafka_message(msg: Any) -> dict[str, Any]:
    raw_value = msg.value().decode("utf-8", errors="replace")

    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        payload = {
            "parse_error": str(exc),
            "raw_value": raw_value,
        }

    if not isinstance(payload, dict):
        payload = {"value": payload}

    payload.update(
        {
            "kafka_topic": msg.topic(),
            "kafka_partition": msg.partition(),
            "kafka_offset": msg.offset(),
            "kafka_key": msg.key().decode("utf-8", errors="replace")
            if msg.key()
            else None,
            "dashboard_received_at": now_utc().isoformat(),
        }
    )
    return payload


def init_session_state() -> None:
    defaults = {
        "raw_trades": deque(maxlen=RAW_BUFFER_LIMIT),
        "features": deque(maxlen=FEATURE_BUFFER_LIMIT),
        "dlq_messages": deque(maxlen=DLQ_BUFFER_LIMIT),
        "topic_counts": Counter(),
        "latest_offsets": {},
        "last_message_time": {},
        "poll_errors": deque(maxlen=20),
        "raw_receive_times": deque(maxlen=1_000),
        "topic_receive_events": deque(maxlen=TELEMETRY_BUFFER_LIMIT),
        "latency_samples": deque(maxlen=TELEMETRY_BUFFER_LIMIT),
        "consumer": None,
        "consumer_topics": None,
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def ensure_consumer(topics: list[str]) -> Consumer | None:
    selected_topics = tuple(sorted(topics))

    if (
        st.session_state.consumer is not None
        and st.session_state.consumer_topics == selected_topics
    ):
        return st.session_state.consumer

    if st.session_state.consumer is not None:
        st.session_state.consumer.close()

    try:
        group_suffix = "-".join(topic.replace(".", "-") for topic in selected_topics)
        consumer = create_consumer(
            group_id=f"streamlit-dashboard-{group_suffix}",
            topics=topics,
        )
    except Exception as exc:
        st.session_state.poll_errors.append(
            f"{now_utc().isoformat()} | Kafka consumer setup failed: {exc}"
        )
        st.session_state.consumer = None
        st.session_state.consumer_topics = None
        return None

    st.session_state.consumer = consumer
    st.session_state.consumer_topics = selected_topics
    return consumer


def poll_messages(consumer: Consumer | None, max_messages: int = POLL_BATCH_SIZE) -> list[dict[str, Any]]:
    if consumer is None:
        return []

    records = []

    for _ in range(max_messages):
        try:
            msg = consumer.poll(POLL_TIMEOUT_SECONDS)
        except KafkaException as exc:
            st.session_state.poll_errors.append(
                f"{now_utc().isoformat()} | Kafka poll failed: {exc}"
            )
            break

        if msg is None:
            break

        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                st.session_state.poll_errors.append(
                    f"{now_utc().isoformat()} | {msg.topic()}: {msg.error()}"
                )
            continue

        records.append(normalize_kafka_message(msg))

    return records


def update_session_buffers(records: list[dict[str, Any]]) -> None:
    for record in records:
        topic = record.get("kafka_topic")
        partition = record.get("kafka_partition")
        received_at = parse_timestamp(record.get("dashboard_received_at")) or now_utc()

        st.session_state.topic_counts[topic] += 1
        st.session_state.latest_offsets[(topic, partition)] = record.get("kafka_offset")
        st.session_state.last_message_time[topic] = record.get("dashboard_received_at")
        st.session_state.topic_receive_events.append(
            {
                "topic": topic,
                "received_at": received_at.isoformat(),
                "received_at_epoch": received_at.timestamp(),
            }
        )

        if topic in {RAW_TRADE_TOPIC, MOCK_TRADE_TOPIC}:
            st.session_state.raw_trades.append(record)
            st.session_state.raw_receive_times.append(time.time())
            record_raw_latency_sample(record, received_at)
        elif topic == FEATURE_TOPIC:
            st.session_state.features.append(record)
            record_feature_latency_sample(record, received_at)
        elif topic == DLQ_TOPIC:
            st.session_state.dlq_messages.append(record)


def milliseconds_between(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None

    return max(0.0, (end - start).total_seconds() * 1_000)


def record_raw_latency_sample(record: dict[str, Any], received_at: datetime) -> None:
    event_at = parse_timestamp(record.get("event_timestamp"))
    ingest_at = parse_timestamp(record.get("ingest_timestamp"))

    st.session_state.latency_samples.append(
        {
            "kind": "raw_trade",
            "topic": record.get("kafka_topic"),
            "symbol": record.get("symbol"),
            "source": record.get("source"),
            "observed_at": received_at.isoformat(),
            "event_to_ingest_ms": milliseconds_between(event_at, ingest_at),
            "ingest_to_dashboard_ms": milliseconds_between(ingest_at, received_at),
            "event_to_dashboard_ms": milliseconds_between(event_at, received_at),
            "feature_to_dashboard_ms": None,
        }
    )


def record_feature_latency_sample(record: dict[str, Any], received_at: datetime) -> None:
    feature_at = parse_timestamp(record.get("feature_timestamp"))

    st.session_state.latency_samples.append(
        {
            "kind": "feature",
            "topic": record.get("kafka_topic"),
            "symbol": record.get("symbol"),
            "source": record.get("source"),
            "observed_at": received_at.isoformat(),
            "event_to_ingest_ms": None,
            "ingest_to_dashboard_ms": None,
            "event_to_dashboard_ms": None,
            "feature_to_dashboard_ms": milliseconds_between(feature_at, received_at),
        }
    )


def records_to_df(records: deque, columns: list[str] | None = None) -> pd.DataFrame:
    df = pd.DataFrame(list(records))

    if df.empty:
        return pd.DataFrame(columns=columns or [])

    if columns:
        for column in columns:
            if column not in df.columns:
                df[column] = None
        df = df[columns]

    return df


def coerce_numeric_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def estimate_messages_per_second() -> float:
    cutoff = time.time() - 60
    receive_times = [ts for ts in st.session_state.raw_receive_times if ts >= cutoff]
    return len(receive_times) / 60


def build_throughput_summary() -> pd.DataFrame:
    cutoff = time.time() - THROUGHPUT_WINDOW_SECONDS
    events = [
        event
        for event in st.session_state.topic_receive_events
        if event["received_at_epoch"] >= cutoff
    ]

    if not events:
        return pd.DataFrame(columns=["topic", "messages_last_60s", "messages_per_second"])

    df = pd.DataFrame(events)
    summary = (
        df.groupby("topic")
        .size()
        .reset_index(name="messages_last_60s")
        .sort_values("topic")
    )
    summary["messages_per_second"] = summary["messages_last_60s"] / THROUGHPUT_WINDOW_SECONDS
    return summary


def build_throughput_timeseries(bucket: str = "10s") -> pd.DataFrame:
    events = list(st.session_state.topic_receive_events)
    if not events:
        return pd.DataFrame(columns=["received_bucket", "topic", "messages"])

    df = pd.DataFrame(events)
    df["received_at"] = pd.to_datetime(df["received_at"], errors="coerce", utc=True)
    df = df.dropna(subset=["received_at", "topic"])

    if df.empty:
        return pd.DataFrame(columns=["received_bucket", "topic", "messages"])

    df["received_bucket"] = df["received_at"].dt.floor(bucket)
    return (
        df.groupby(["received_bucket", "topic"])
        .size()
        .reset_index(name="messages")
        .sort_values("received_bucket")
    )


def build_latency_summary() -> pd.DataFrame:
    samples = list(st.session_state.latency_samples)
    if not samples:
        return pd.DataFrame(columns=["latency", "latest_ms", "avg_ms", "p95_ms"])

    df = pd.DataFrame(samples)
    rows = []
    latency_columns = {
        "Raw event -> ingest": "event_to_ingest_ms",
        "Raw ingest -> dashboard": "ingest_to_dashboard_ms",
        "Raw event -> dashboard": "event_to_dashboard_ms",
        "Feature -> dashboard": "feature_to_dashboard_ms",
    }

    for label, column in latency_columns.items():
        values = pd.to_numeric(df[column], errors="coerce").dropna()
        if values.empty:
            continue

        rows.append(
            {
                "latency": label,
                "latest_ms": round(values.iloc[-1], 2),
                "avg_ms": round(values.mean(), 2),
                "p95_ms": round(values.quantile(0.95), 2),
                "samples": len(values),
            }
        )

    return pd.DataFrame(rows)


def build_dashboard_lag_df(consumer: Consumer | None) -> pd.DataFrame:
    if consumer is None:
        return pd.DataFrame(
            columns=[
                "topic",
                "partition",
                "low_watermark",
                "high_watermark",
                "consumer_position",
                "latest_dashboard_offset",
                "lag_messages",
            ]
        )

    rows = []

    try:
        assignments = consumer.assignment()
        positions = {
            (tp.topic, tp.partition): tp.offset
            for tp in consumer.position(assignments)
        }
    except KafkaException as exc:
        st.session_state.poll_errors.append(
            f"{now_utc().isoformat()} | Kafka lag lookup failed: {exc}"
        )
        assignments = []
        positions = {}

    for tp in assignments:
        latest_offset = st.session_state.latest_offsets.get((tp.topic, tp.partition))

        try:
            low_watermark, high_watermark = consumer.get_watermark_offsets(
                TopicPartition(tp.topic, tp.partition),
                timeout=0.2,
                cached=False,
            )
        except KafkaException as exc:
            st.session_state.poll_errors.append(
                f"{now_utc().isoformat()} | Kafka watermark lookup failed for {tp.topic}[{tp.partition}]: {exc}"
            )
            low_watermark, high_watermark = None, None

        position = positions.get((tp.topic, tp.partition))
        if position is None or position < 0:
            position = latest_offset + 1 if latest_offset is not None else None

        lag = None
        if high_watermark is not None and position is not None:
            lag = max(0, high_watermark - position)

        rows.append(
            {
                "topic": tp.topic,
                "partition": tp.partition,
                "low_watermark": low_watermark,
                "high_watermark": high_watermark,
                "consumer_position": position,
                "latest_dashboard_offset": latest_offset,
                "lag_messages": lag,
            }
        )

    return pd.DataFrame(rows).sort_values(["topic", "partition"]) if rows else pd.DataFrame()


def apply_dark_theme() -> None:
    st.set_page_config(
        page_title="Market Data Pipeline Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.markdown(
        """
        <style>
        :root {
            --bg: #080d14;
            --panel: #101722;
            --panel-soft: #141d2b;
            --text: #e5edf7;
            --muted: #8ea0b7;
            --accent: #2dd4bf;
            --accent-2: #38bdf8;
            --danger: #fb7185;
        }

        .stApp {
            background: linear-gradient(180deg, #080d14 0%, #0b111b 100%);
            color: var(--text);
        }

        [data-testid="stSidebar"] {
            background: #080d14;
            border-right: 1px solid rgba(148, 163, 184, 0.16);
        }

        [data-testid="stMetric"] {
            background: linear-gradient(180deg, rgba(20, 29, 43, 0.96), rgba(13, 20, 31, 0.96));
            border: 1px solid rgba(148, 163, 184, 0.15);
            border-radius: 8px;
            padding: 16px 18px;
            box-shadow: 0 12px 30px rgba(0, 0, 0, 0.24);
        }

        [data-testid="stMetricLabel"] p {
            color: var(--muted);
            font-size: 0.82rem;
        }

        [data-testid="stMetricValue"] {
            color: var(--text);
        }

        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
        }

        .dashboard-subtitle {
            color: var(--muted);
            font-size: 1.05rem;
            margin-bottom: 1.5rem;
        }

        .health-pill {
            display: inline-flex;
            align-items: center;
            gap: 0.45rem;
            border-radius: 999px;
            padding: 0.45rem 0.75rem;
            font-weight: 600;
            border: 1px solid rgba(148, 163, 184, 0.18);
            background: rgba(20, 29, 43, 0.9);
        }

        .flowing { color: var(--accent); }
        .stale { color: var(--danger); }

        .session-card {
            background: linear-gradient(180deg, rgba(20, 29, 43, 0.96), rgba(13, 20, 31, 0.96));
            border: 1px solid rgba(148, 163, 184, 0.15);
            border-radius: 8px;
            padding: 16px 18px;
            margin-bottom: 1.4rem;
            box-shadow: 0 12px 30px rgba(0, 0, 0, 0.22);
        }

        .session-grid {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 1rem;
        }

        .session-label {
            color: var(--muted);
            font-size: 0.78rem;
            margin-bottom: 0.28rem;
        }

        .session-value {
            color: var(--text);
            font-size: 1rem;
            font-weight: 650;
        }

        .session-open { color: var(--accent); }
        .session-closed { color: var(--danger); }

        @media (max-width: 900px) {
            .session-grid {
                grid-template-columns: repeat(2, minmax(0, 1fr));
            }
        }

        div[data-testid="stDataFrame"] {
            border: 1px solid rgba(148, 163, 184, 0.13);
            border-radius: 8px;
            overflow: hidden;
        }

        h1, h2, h3 {
            letter-spacing: 0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_header(refresh_seconds: int) -> None:
    st.title("Real-Time Market Data Pipeline Dashboard")
    st.markdown(
        '<div class="dashboard-subtitle">Kafka-powered trade ingestion, feature engineering, and monitoring</div>',
        unsafe_allow_html=True,
    )

    if st_autorefresh is not None:
        st_autorefresh(interval=refresh_seconds * 1_000, key="dashboard_refresh")
    else:
        st.info(
            "Install streamlit-autorefresh for automatic refreshes, or rerun the app manually."
        )


def render_market_session_card(data_source_mode: str, raw_df: pd.DataFrame, feature_df: pd.DataFrame) -> None:
    session = get_market_session_state()
    status_class = "session-open" if session["status"] == "Open" else "session-closed"

    symbols = sorted(
        set(raw_df["symbol"].dropna().tolist() if "symbol" in raw_df else [])
        | set(feature_df["symbol"].dropna().tolist() if "symbol" in feature_df else [])
    )
    symbol_text = html.escape(", ".join(symbols) if symbols else "Waiting for symbols")
    mode_text = html.escape(data_source_mode)
    status_text = html.escape(session["status"])
    detail_text = html.escape(session["detail"])

    st.markdown(
        f"""
        <div class="session-card">
          <div class="session-grid">
            <div>
              <div class="session-label">Market Session</div>
              <div class="session-value {status_class}">{status_text}</div>
            </div>
            <div>
              <div class="session-label">Next Event</div>
              <div class="session-value">{detail_text}</div>
            </div>
            <div>
              <div class="session-label">Data Mode</div>
              <div class="session-value">{mode_text}</div>
            </div>
            <div>
              <div class="session-label">Active Symbols</div>
              <div class="session-value">{symbol_text}</div>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_raw_trades_tab(raw_df: pd.DataFrame) -> None:
    st.subheader("Raw Trade Stream")

    latest = raw_df.iloc[-1].to_dict() if not raw_df.empty else {}

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total trades received", f"{len(raw_df):,}")
    col2.metric("Latest trade symbol", latest.get("symbol", "N/A"))
    col3.metric("Latest trade price", format_number(latest.get("price"), 4, "$"))
    col4.metric("Messages/sec estimate", f"{estimate_messages_per_second():.2f}")

    filter_col1, filter_col2 = st.columns(2)
    symbols = sorted(raw_df["symbol"].dropna().unique()) if not raw_df.empty else []
    sources = sorted(raw_df["source"].dropna().unique()) if not raw_df.empty else []

    selected_symbols = filter_col1.multiselect("Symbol", symbols, default=symbols)
    selected_sources = filter_col2.multiselect("Source", sources, default=sources)

    filtered = raw_df.copy()
    if selected_symbols:
        filtered = filtered[filtered["symbol"].isin(selected_symbols)]
    if selected_sources:
        filtered = filtered[filtered["source"].isin(selected_sources)]

    st.dataframe(
        filtered.sort_values(["event_timestamp", "kafka_offset"], na_position="first", ascending=False),
        width="stretch",
        hide_index=True,
    )


def render_features_tab(feature_df: pd.DataFrame) -> None:
    st.subheader("Calculated Feature Stream")

    latest = feature_df.iloc[-1].to_dict() if not feature_df.empty else {}

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Latest feature symbol", latest.get("symbol", "N/A"))
    col2.metric("Latest rolling VWAP", format_number(latest.get("rolling_vwap_1m"), 4, "$"))
    col3.metric("Latest 1-minute return", format_percent(latest.get("return_1m")))
    col4.metric("Latest 5-minute realized volatility", format_percent(latest.get("realized_vol_5m")))

    st.dataframe(
        feature_df.sort_values(["feature_timestamp"], na_position="first", ascending=False),
        width="stretch",
        hide_index=True,
    )


def build_feature_aggregates(feature_df: pd.DataFrame) -> pd.DataFrame:
    if feature_df.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "latest_price",
                "avg_rolling_vwap_1m",
                "avg_trade_count_1m",
                "avg_volume_1m",
                "latest_return_1m",
                "latest_realized_vol_5m",
            ]
        )

    df = feature_df.copy()
    df["feature_timestamp_dt"] = pd.to_datetime(df["feature_timestamp"], errors="coerce", utc=True)
    df = df.sort_values("feature_timestamp_dt")

    latest = df.groupby("symbol", as_index=False).tail(1).set_index("symbol")
    averages = df.groupby("symbol").agg(
        avg_rolling_vwap_1m=("rolling_vwap_1m", "mean"),
        avg_trade_count_1m=("trade_count_1m", "mean"),
        avg_volume_1m=("volume_1m", "mean"),
    )

    result = averages.join(
        latest[
            [
                "last_trade_price",
                "return_1m",
                "realized_vol_5m",
            ]
        ]
    ).reset_index()

    return result.rename(
        columns={
            "last_trade_price": "latest_price",
            "return_1m": "latest_return_1m",
            "realized_vol_5m": "latest_realized_vol_5m",
        }
    )


def render_line_chart(feature_df: pd.DataFrame, y_column: str, title: str) -> None:
    if feature_df.empty or y_column not in feature_df.columns:
        st.info(f"No data yet for {title}.")
        return

    chart_df = feature_df.copy()
    chart_df["feature_timestamp"] = pd.to_datetime(
        chart_df["feature_timestamp"],
        errors="coerce",
        utc=True,
    )
    chart_df = chart_df.dropna(subset=["feature_timestamp", y_column, "symbol"])

    if chart_df.empty:
        st.info(f"No plottable data yet for {title}.")
        return

    fig = px.line(
        chart_df,
        x="feature_timestamp",
        y=y_column,
        color="symbol",
        title=title,
        template="plotly_dark",
        markers=False,
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend_title_text="Symbol",
        margin=dict(l=20, r=20, t=55, b=20),
    )
    st.plotly_chart(fig, width="stretch")


def build_return_heatmap(feature_df: pd.DataFrame, bucket: str) -> pd.DataFrame:
    if feature_df.empty:
        return pd.DataFrame()

    heatmap_df = feature_df.copy()
    heatmap_df["feature_timestamp"] = pd.to_datetime(
        heatmap_df["feature_timestamp"],
        errors="coerce",
        utc=True,
    )
    heatmap_df = heatmap_df.dropna(subset=["feature_timestamp", "symbol", "return_1m"])

    if heatmap_df.empty:
        return pd.DataFrame()

    heatmap_df["time_bucket"] = heatmap_df["feature_timestamp"].dt.floor(bucket)
    heatmap_df["return_pct"] = heatmap_df["return_1m"] * 100

    return heatmap_df.pivot_table(
        index="symbol",
        columns="time_bucket",
        values="return_pct",
        aggfunc="last",
    ).sort_index()


def render_return_heatmap(feature_df: pd.DataFrame) -> None:
    st.markdown("**1-Minute Return Heatmap**")

    bucket = st.segmented_control(
        "Heatmap bucket",
        options=["15s", "30s", "1min"],
        default="30s",
        key="return_heatmap_bucket",
    )

    heatmap = build_return_heatmap(feature_df, bucket)

    if heatmap.empty:
        st.info("No return data available yet for the selected symbols.")
        return

    labels = [ts.strftime("%H:%M:%S") for ts in heatmap.columns]
    fig = px.imshow(
        heatmap,
        x=labels,
        y=heatmap.index,
        color_continuous_scale="RdYlGn",
        color_continuous_midpoint=0,
        aspect="auto",
        labels=dict(x="Time bucket", y="Symbol", color="Return 1m (%)"),
        template="plotly_dark",
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=20, r=20, t=20, b=20),
    )
    fig.update_xaxes(tickangle=0)
    st.plotly_chart(fig, width="stretch")


def render_aggregates_tab(feature_df: pd.DataFrame) -> None:
    st.subheader("Symbol-Level Feature Aggregates")

    available_symbols = sorted(feature_df["symbol"].dropna().unique()) if not feature_df.empty else []
    selected_symbols = st.multiselect(
        "Symbols",
        options=available_symbols,
        default=available_symbols,
        key="aggregate_symbols",
    )

    filtered_features = feature_df.copy()
    if selected_symbols:
        filtered_features = filtered_features[filtered_features["symbol"].isin(selected_symbols)]
    else:
        filtered_features = filtered_features.iloc[0:0]

    aggregates = build_feature_aggregates(filtered_features)
    st.dataframe(aggregates, width="stretch", hide_index=True)

    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        render_line_chart(filtered_features, "last_trade_price", "Last Trade Price by Symbol")
    with chart_col2:
        render_line_chart(filtered_features, "rolling_vwap_1m", "Rolling VWAP 1m by Symbol")

    render_line_chart(filtered_features, "volume_1m", "Volume 1m by Symbol")
    render_return_heatmap(filtered_features)


def topic_health_rows() -> pd.DataFrame:
    rows = []
    topics = sorted(set(st.session_state.topic_counts) | set(st.session_state.last_message_time))

    for topic in topics:
        last_seen = parse_timestamp(st.session_state.last_message_time.get(topic))
        age_seconds = (now_utc() - last_seen).total_seconds() if last_seen else None
        rows.append(
            {
                "topic": topic,
                "messages_consumed": st.session_state.topic_counts.get(topic, 0),
                "last_message_time": st.session_state.last_message_time.get(topic),
                "seconds_since_last_message": round(age_seconds, 2)
                if age_seconds is not None
                else None,
                "flowing": bool(age_seconds is not None and age_seconds <= FLOWING_WINDOW_SECONDS),
            }
        )

    return pd.DataFrame(rows)


def offsets_df() -> pd.DataFrame:
    rows = [
        {
            "topic": topic,
            "partition": partition,
            "latest_offset": offset,
        }
        for (topic, partition), offset in st.session_state.latest_offsets.items()
    ]
    return pd.DataFrame(rows).sort_values(["topic", "partition"]) if rows else pd.DataFrame()


def render_latency_panel() -> None:
    latency_df = build_latency_summary()

    st.markdown("**Latency**")
    if latency_df.empty:
        st.info("No latency samples available yet.")
        return

    metric_cols = st.columns(min(4, len(latency_df)))
    for index, row in latency_df.head(4).iterrows():
        metric_cols[index].metric(
            row["latency"],
            f"{row['latest_ms']:,.0f} ms",
            delta=f"avg {row['avg_ms']:,.0f} ms",
            delta_color="off",
        )

    st.dataframe(latency_df, width="stretch", hide_index=True)


def render_throughput_panel() -> None:
    st.markdown("**Throughput**")

    throughput_summary = build_throughput_summary()
    throughput_ts = build_throughput_timeseries()

    if throughput_summary.empty:
        st.info("No throughput samples available yet.")
        return

    fig = px.line(
        throughput_ts,
        x="received_bucket",
        y="messages",
        color="topic",
        title="Messages per 10 seconds",
        template="plotly_dark",
        markers=True,
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend_title_text="Topic",
        margin=dict(l=20, r=20, t=55, b=20),
    )
    st.plotly_chart(fig, width="stretch")
    st.dataframe(throughput_summary, width="stretch", hide_index=True)


def render_lag_panel(consumer: Consumer | None) -> None:
    st.markdown("**Dashboard Consumer Lag**")
    lag_df = build_dashboard_lag_df(consumer)

    if lag_df.empty:
        st.info("No Kafka partition assignment available yet.")
        return

    total_lag = pd.to_numeric(lag_df["lag_messages"], errors="coerce").fillna(0).sum()
    max_lag = pd.to_numeric(lag_df["lag_messages"], errors="coerce").fillna(0).max()
    lag_col1, lag_col2, lag_col3 = st.columns(3)
    lag_col1.metric("Total lag", f"{int(total_lag):,} messages")
    lag_col2.metric("Max partition lag", f"{int(max_lag):,} messages")
    lag_col3.metric("Assigned partitions", f"{len(lag_df):,}")

    st.dataframe(lag_df, width="stretch", hide_index=True)


def render_health_tab(dlq_df: pd.DataFrame, consumer: Consumer | None) -> None:
    st.subheader("Pipeline Health")

    health_df = topic_health_rows()
    flowing = bool(not health_df.empty and health_df["flowing"].any())
    pill_class = "flowing" if flowing else "stale"
    pill_text = "Data flowing" if flowing else "Waiting for fresh messages"

    st.markdown(
        f'<span class="health-pill {pill_class}">{pill_text}</span>',
        unsafe_allow_html=True,
    )

    st.write("")

    render_latency_panel()
    render_throughput_panel()
    render_lag_panel(consumer)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Messages by Topic**")
        st.dataframe(health_df, width="stretch", hide_index=True)

    with col2:
        st.markdown("**Latest Kafka Offsets**")
        st.dataframe(offsets_df(), width="stretch", hide_index=True)

    st.markdown("**Recent DLQ Messages**")
    if dlq_df.empty:
        st.info("No DLQ messages consumed in this dashboard session.")
    else:
        st.dataframe(
            dlq_df.sort_values("dashboard_received_at", ascending=False),
            width="stretch",
            hide_index=True,
        )

    if st.session_state.poll_errors:
        with st.expander("Kafka connection and poll warnings", expanded=False):
            for error in reversed(st.session_state.poll_errors):
                st.warning(error)


def render_sidebar() -> tuple[str, int, int]:
    st.sidebar.header("Stream Controls")
    data_source_mode = st.sidebar.radio(
        "Data Source Mode",
        options=list(DATA_SOURCE_TOPICS.keys()),
        index=0,
    )
    refresh_seconds = st.sidebar.slider(
        "Refresh interval",
        min_value=2,
        max_value=15,
        value=3,
        step=1,
    )
    table_limit = st.sidebar.slider(
        "Rows displayed",
        min_value=50,
        max_value=1_000,
        value=250,
        step=50,
    )

    st.sidebar.caption(f"Kafka bootstrap: `{BOOTSTRAP_SERVERS}`")
    st.sidebar.caption("Offsets: `latest`")
    return data_source_mode, refresh_seconds, table_limit


def main() -> None:
    apply_dark_theme()
    init_session_state()

    data_source_mode, refresh_seconds, table_limit = render_sidebar()
    render_header(refresh_seconds)

    topics = DATA_SOURCE_TOPICS[data_source_mode] + [FEATURE_TOPIC, DLQ_TOPIC]
    consumer = ensure_consumer(topics)
    records = poll_messages(consumer)
    update_session_buffers(records)

    raw_df = records_to_df(st.session_state.raw_trades, RAW_TRADE_COLUMNS)
    raw_df = raw_df[raw_df["kafka_topic"].isin(DATA_SOURCE_TOPICS[data_source_mode])]
    raw_df = raw_df.tail(table_limit)
    feature_df = records_to_df(st.session_state.features, FEATURE_COLUMNS)
    feature_df = coerce_numeric_columns(feature_df, FEATURE_NUMERIC_COLUMNS).tail(table_limit)
    dlq_df = records_to_df(st.session_state.dlq_messages).tail(table_limit)

    render_market_session_card(data_source_mode, raw_df, feature_df)

    raw_tab, features_tab, aggregates_tab, health_tab = st.tabs(
        ["Raw Trades", "Feature Stream", "Aggregates", "Pipeline Health"]
    )

    with raw_tab:
        render_raw_trades_tab(raw_df)

    with features_tab:
        render_features_tab(feature_df)

    with aggregates_tab:
        render_aggregates_tab(feature_df)

    with health_tab:
        render_health_tab(dlq_df, consumer)


if __name__ == "__main__":
    main()
