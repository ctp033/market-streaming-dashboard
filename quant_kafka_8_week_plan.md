# 8-Week Build Plan: Real-Time Quant Kafka Market Data Pipeline

## Project goal

Build a **real-time equities market data pipeline** that:

- ingests live trades and quotes
- publishes them into Kafka
- computes real-time microstructure features
- stores raw and processed data
- supports replay/backtesting
- exposes metrics and simple dashboards

At the end, the project should look like:

> Built a Kafka-based streaming market data platform for ingesting live trade/quote events, computing intraday microstructure features, and serving research-ready datasets for analytics and backtesting.

---

## Final architecture

```text
                ┌──────────────────────┐
                │ Market Data Provider │
                │  (Polygon WebSocket) │
                └──────────┬───────────┘
                           │
                           ▼
                ┌──────────────────────┐
                │   Python Producer    │
                │ websocket_ingestor   │
                └──────────┬───────────┘
                           │
                           ▼
                  ┌───────────────────┐
                  │       Kafka       │
                  │   raw topics      │
                  └───────┬───────────┘
                          │
         ┌────────────────┴────────────────┐
         │                                 │
         ▼                                 ▼
┌──────────────────────┐         ┌──────────────────────┐
│ Feature Consumer     │         │ Raw Sink Consumer    │
│ computes indicators  │         │ writes raw parquet   │
└──────────┬───────────┘         └──────────┬───────────┘
           │                                │
           ▼                                ▼
  ┌──────────────────────┐         ┌──────────────────────┐
  │ Kafka feature topics │         │ Data lake / parquet  │
  └──────────┬───────────┘         └──────────────────────┘
             │
             ▼
  ┌──────────────────────┐
  │ Feature Sink         │
  │ Postgres / DuckDB    │
  └──────────┬───────────┘
             │
             ▼
  ┌──────────────────────┐
  │ Research / Dashboard │
  │ notebooks + metrics  │
  └──────────────────────┘
```

---

## Kafka topics

### Raw topics
- `market.trades.raw`
- `market.quotes.raw`
- `market.bars.1m.raw` *(optional early on)*

### Processed topics
- `market.features.microstructure`
- `market.signals.intraday` *(optional later)*

### Ops topics
- `market.errors.dlq`
- `market.ingestion.heartbeat`

### Replay topics
- `market.trades.replay`
- `market.quotes.replay`

### Minimum version 1 topics
For the first working version, only use:
- `market.trades.raw`
- `market.quotes.raw`
- `market.features.microstructure`
- `market.errors.dlq`

---

## Suggested event schemas

### Trade event
```json
{
  "symbol": "AAPL",
  "event_type": "trade",
  "price": 212.31,
  "size": 100,
  "exchange": 11,
  "sip_timestamp": 1713892012345678900,
  "ingest_timestamp": "2026-04-23T15:10:22.134Z"
}
```

### Quote event
```json
{
  "symbol": "AAPL",
  "event_type": "quote",
  "bid_price": 212.30,
  "bid_size": 400,
  "ask_price": 212.32,
  "ask_size": 500,
  "exchange": 11,
  "sip_timestamp": 1713892012345679900,
  "ingest_timestamp": "2026-04-23T15:10:22.210Z"
}
```

### Feature event
```json
{
  "symbol": "AAPL",
  "feature_timestamp": "2026-04-23T15:10:30Z",
  "midprice": 212.31,
  "spread": 0.02,
  "spread_bps": 0.94,
  "rolling_vwap_1m": 212.28,
  "return_1m": 0.0018,
  "realized_vol_5m": 0.012,
  "quote_imbalance": 0.1111,
  "trade_count_1m": 37,
  "volume_1m": 41200
}
```

---

## Folder structure

```text
quant-kafka-pipeline/
│
├── README.md
├── docker-compose.yml
├── .env
├── requirements.txt
├── pyproject.toml
├── Makefile
│
├── config/
│   ├── app_config.yaml
│   ├── kafka_config.yaml
│   └── symbols.yaml
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── replay/
│
├── docs/
│   ├── architecture.md
│   ├── topic_design.md
│   ├── schemas.md
│   └── runbook.md
│
├── infra/
│   ├── kafka/
│   │   └── init_topics.sh
│   ├── postgres/
│   │   └── init.sql
│   └── monitoring/
│       └── prometheus.yml
│
├── src/
│   ├── common/
│   │   ├── logger.py
│   │   ├── settings.py
│   │   ├── schemas.py
│   │   ├── time_utils.py
│   │   └── kafka_utils.py
│   │
│   ├── producers/
│   │   └── polygon_ws_producer.py
│   │
│   ├── consumers/
│   │   ├── raw_sink_consumer.py
│   │   ├── feature_engine_consumer.py
│   │   ├── feature_sink_consumer.py
│   │   └── replay_producer.py
│   │
│   ├── features/
│   │   ├── rolling_windows.py
│   │   ├── vwap.py
│   │   ├── volatility.py
│   │   ├── returns.py
│   │   ├── spread.py
│   │   └── imbalance.py
│   │
│   ├── storage/
│   │   ├── parquet_writer.py
│   │   ├── postgres_writer.py
│   │   └── duckdb_writer.py
│   │
│   ├── services/
│   │   ├── market_data_service.py
│   │   ├── feature_service.py
│   │   └── health_service.py
│   │
│   └── api/
│       └── app.py
│
├── notebooks/
│   ├── 01_validate_raw_data.ipynb
│   ├── 02_feature_exploration.ipynb
│   └── 03_simple_signal_backtest.ipynb
│
├── dashboards/
│   └── streamlit_app.py
│
├── tests/
│   ├── test_schemas.py
│   ├── test_vwap.py
│   ├── test_returns.py
│   ├── test_volatility.py
│   └── test_pipeline_smoke.py
│
└── scripts/
    ├── run_producer.sh
    ├── run_consumers.sh
    ├── seed_replay_data.sh
    └── create_topics.sh
```

---

## Milestone order

Build in this order:

1. Local Kafka environment
2. Mock producer with fake trade data
3. Consumer that reads and logs messages
4. Live market-data ingestion
5. Raw storage sink
6. Feature engine
7. Feature storage
8. Notebook analysis
9. Replay functionality
10. Monitoring and polish

This order matters because you want to learn Kafka first without fighting live APIs immediately.

---

## Week 1 — Environment and Kafka foundations

### Goal
Get a local dev environment running and understand the data flow.

### Build
- Install Docker and Docker Compose
- Start Kafka locally
- Start optional Postgres locally
- Create topics manually
- Build a tiny producer that sends fake trade events
- Build a tiny consumer that reads and prints them

### Deliverables
- `docker-compose.yml`
- `mock_producer.py`
- `mock_consumer.py`
- topic creation script

### What you should learn
- brokers
- topics
- partitions
- offsets
- consumer groups
- serialization basics

### Milestone
You can run one producer and one consumer and see data flowing through Kafka.

---

## Week 2 — Market data ingestion

### Goal
Replace fake events with real or semi-real market data.

### Build
- Connect to Polygon WebSocket or another feed
- Subscribe to a small symbol universe first:
  - AAPL
  - MSFT
  - SPY
  - NVDA
- Parse incoming trade and quote messages
- Normalize them into your schema
- Publish to:
  - `market.trades.raw`
  - `market.quotes.raw`

### Keep scope small
Do not ingest the whole market yet.

### Deliverables
- `polygon_ws_producer.py`
- normalized schemas
- config-driven symbol list

### Milestone
Live data is streaming into Kafka topics.

---

## Week 3 — Raw sink and data lake layer

### Goal
Persist raw events for later replay and research.

### Build
- Consumer that reads raw trades and quotes
- Write them to partitioned parquet files
- Partition by:
  - date
  - symbol
  - event type

Example path:

```text
data/raw/event_type=trade/symbol=AAPL/date=2026-04-23/part-000.parquet
```

### Add
- batch flush logic every N records or every X seconds
- dead-letter handling for malformed messages

### Deliverables
- `raw_sink_consumer.py`
- `parquet_writer.py`
- local raw data files

### Milestone
You can stop the pipeline and still have research-ready raw data on disk.

---

## Week 4 — Feature engine v1

### Goal
Compute useful quant-style streaming features.

### Build
A consumer that:
- reads trades and quotes
- maintains rolling state per symbol
- emits feature events every few seconds or every minute

### Start with these features
- midprice
- spread
- spread in bps
- rolling VWAP over 1 minute
- rolling return over 1 minute
- trade count over 1 minute
- volume over 1 minute
- quote imbalance

Formula:

```text
(bid size - ask size) / (bid size + ask size)
```

### Output topic
- `market.features.microstructure`

### Deliverables
- `feature_engine_consumer.py`
- feature modules under `src/features/`

### Milestone
You now have a real streaming research feature pipeline.

---

## Week 5 — Feature storage and analysis layer

### Goal
Make the processed data easy to query.

### Build
- consumer for `market.features.microstructure`
- write to either:
  - **DuckDB** for simplicity, or
  - **Postgres** for realism

### Recommendation
- Use **DuckDB first** if you want speed and less setup
- Use **Postgres** if you want more production database experience

### Create tables like
- `raw_trades_summary`
- `raw_quotes_summary`
- `microstructure_features`

### Deliverables
- `feature_sink_consumer.py`
- analytical tables
- simple queries for recent features

### Milestone
You can query features with SQL.

---

## Week 6 — Research notebook and simple signal study

### Goal
Show this is useful for quant analysis, not just engineering.

### Build notebooks that test ideas like:
- does spread widening predict near-term volatility?
- does quote imbalance predict the next 1-minute return?
- how do VWAP deviations behave intraday?
- do volume spikes cluster before larger price moves?

### Produce
- summary plots
- correlation tables
- basic backtest of a toy signal

### Example toy signal
- long when quote imbalance > threshold and spread is below median
- flat otherwise

Do not worry about making the signal profitable. The point is to show **research workflow enablement**.

### Deliverables
- `03_simple_signal_backtest.ipynb`
- a few plots and conclusions

### Milestone
Your project now has a quant/research story.

---

## Week 7 — Replay and robustness

### Goal
Make the pipeline more realistic and reliable.

### Build
- replay producer that reads historical parquet
- republishes messages into replay topics
- ability to rerun feature generation on old data

### Add robustness
- retries
- logging
- message validation
- dead-letter queue
- idempotent write behavior where possible
- heartbeat messages

### Deliverables
- `replay_producer.py`
- `market.trades.replay`
- `market.quotes.replay`
- DLQ support

### Milestone
You can backfill and test pipeline behavior without needing a live market session.

---

## Week 8 — Dashboard, monitoring, and resume polish

### Goal
Make it demoable.

### Build
A small dashboard in Streamlit showing:
- latest spread by symbol
- latest midprice
- rolling VWAP
- rolling volatility
- recent message throughput
- consumer lag if available

### Add documentation
- architecture diagram
- topic definitions
- schema docs
- how to run locally
- design tradeoffs
- future improvements

### Deliverables
- `dashboards/streamlit_app.py`
- polished README
- architecture doc
- screenshots

### Milestone
You now have a portfolio-grade project.

---

## Recommended MVP scope

Do this first before adding fancy things.

### MVP features
- 3–5 symbols
- trades and quotes only
- local Kafka with Docker
- one producer
- two consumers
- parquet raw storage
- one processed feature topic
- DuckDB or Postgres storage
- one notebook
- one simple dashboard

That alone is strong enough.

---

## Nice upgrades after MVP

### Upgrade 1
Use **Avro or Protobuf** schemas instead of plain JSON.

### Upgrade 2
Add **Spark Structured Streaming** or **Flink** for feature computation.

### Upgrade 3
Add **dbt** transformations on processed feature tables.

### Upgrade 4
Deploy pieces to AWS:
- MSK or Confluent Cloud
- S3
- ECS or EKS
- RDS Postgres

### Upgrade 5
Add more advanced features:
- realized volatility
- dollar bars
- volume bars
- signed trade imbalance proxy
- intraday z-scores
- regime flags

---

## Recommended tech choices

For the first build, use:
- **Python**
- **Kafka**
- **Docker Compose**
- **JSON first**
- **DuckDB for analytics**
- **Parquet for raw storage**
- **Streamlit for dashboard**
- **Jupyter notebooks for research**

### Why
- fast to build
- easier to debug
- still very credible
- lets you focus on streaming/data engineering instead of infrastructure headaches

---

## Resume framing

### Option 1
**Quant Market Data Streaming Platform**  
Built a real-time Kafka-based equities data pipeline that ingested live trade and quote events, computed intraday microstructure features including spread, VWAP, returns, and quote imbalance, and stored raw and processed datasets for research, replay, and backtesting.

### Option 2
Designed replayable event-driven data workflows with partitioned parquet storage, feature consumers, and SQL-queryable analytics layers to support quant-style exploratory analysis and toy signal evaluation.

---

## What to focus on most

If your goal is quant-firm credibility, the biggest signals are:

- clean event schemas
- good topic design
- understanding of quotes vs trades
- correct rolling-window logic
- replayability
- data quality handling
- solid README and architecture docs
- research tie-in, not just ingestion

---

## Suggested first 3 build sessions

### Session 1
- set up Docker Compose
- launch Kafka
- create one fake producer and one consumer

### Session 2
- define schemas
- create raw trade and raw quote topics
- make fake data look like market data

### Session 3
- connect to live WebSocket
- ingest only AAPL and SPY
- verify data in Kafka and log to disk

This is the cleanest start.
