# Market Streaming Dashboard

Streamlit dashboard for monitoring the local Kafka market-data pipeline.

The dashboard reads:

- raw real trades from `market.trades.raw`
- raw mock trades from `market.trades.mock`
- calculated trade features from `market.features.trade_activity`
- optional DLQ messages from `market.errors.dlq`

## 1. Install dependencies

From the project root:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

If your `.venv` already exists, just run:

```bash
.venv/bin/pip install -r requirements.txt
```

## 2. Run only the dashboard

Use this if Kafka/producers/feature engine are already running:

```bash
.venv/bin/streamlit run dashboards/streamlit_app.py --server.port 8501 --server.address localhost
```

Open:

```text
http://localhost:8501
```

Then choose the desired **Data Source Mode** in the sidebar:

- `Real pipeline`
- `Mock pipeline`
- `Both`

## 3. Run the full mock pipeline

This starts Kafka, the feature engine, the mock trade producer, and the dashboard:

```bash
./scripts/run_mock_pipeline.sh
```

Open:

```text
http://localhost:8501
```

Select **Mock pipeline** in the dashboard sidebar.

If port `8501` is already in use, the script will reuse the existing dashboard. You can also run on a different port:

```bash
STREAMLIT_PORT=8502 ./scripts/run_mock_pipeline.sh
```

## 4. Run the full real pipeline

Add your Finnhub key to `.env`:

```bash
FINNHUB_API_KEY=your_key_here
```

Then run:

```bash
./scripts/run_real_pipeline.sh
```

Open:

```text
http://localhost:8501
```

Select **Real pipeline** in the dashboard sidebar.

If port `8501` is already in use:

```bash
STREAMLIT_PORT=8502 ./scripts/run_real_pipeline.sh
```

## 5. What the dashboard shows

Tabs:

- **Raw Trades**: latest raw trades, filters, total trades, latest price, messages/sec.
- **Feature Stream**: latest calculated feature rows and feature metrics.
- **Aggregates**: symbol-level aggregates, price/VWAP/volume charts, and return heatmap.
- **Pipeline Health**: latency, throughput, dashboard Kafka lag, offsets, topic counts, freshness, and DLQ messages.

## 6. Stopping the pipeline

If you started a full pipeline script, press `Ctrl+C` in that terminal.

Kafka keeps running in Docker. To stop Kafka too:

```bash
docker compose stop kafka
```

To stop all Docker services in this project:

```bash
docker compose down
```

## Troubleshooting

If the dashboard loads but shows no trades:

- Make sure Kafka is running: `docker ps`
- Make sure the producer is running.
- Make sure the sidebar mode matches the producer:
  - mock producer -> `Mock pipeline`
  - Finnhub producer -> `Real pipeline`
- The dashboard starts from latest offsets, so it displays new messages after it starts/subscribes.

If `8501` is already in use:

```bash
STREAMLIT_PORT=8502 ./scripts/run_mock_pipeline.sh
```

or:

```bash
.venv/bin/streamlit run dashboards/streamlit_app.py --server.port 8502 --server.address localhost
```
