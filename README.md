# Crypto Realtime Alert System

An enterprise-style data engineering project that monitors cryptocurrency price movements and correlates them with real-time news sentiment to produce early warning signals.

The system is organized as a monorepo with infrastructure, ingestion, stream processing, and dashboard layers. It uses Kafka for streaming transport, Redis for hot data, ClickHouse for historical storage, and PySpark + FinBERT for real-time analytics.

## Architecture

### Current components

- `infrastructure/`: Docker Compose services for Zookeeper, Kafka, Redis, ClickHouse, Kafka UI, and RedisInsight.
- `data_ingestion/`: Python services that collect market prices from Binance WebSocket and news from CryptoPanic or RSS fallback, then publish raw events to Kafka.
- `stream_processing/`: PySpark Structured Streaming job that reads Kafka topics, applies FinBERT sentiment analysis, enriches price windows with the latest news snapshot, writes live state to Redis, and stores history in ClickHouse.
- `dashboard/`: reserved for the visualization layer.
- `bot-tele/`: placeholder for a future Telegram alerting service.

### Data flow

1. Binance WebSocket publishes raw price events to Kafka topic `raw_prices`.
2. CryptoPanic news polling publishes raw news events to Kafka topic `raw_news`.
3. When CryptoPanic is rate-limited, the ingestion layer falls back to RSS feeds to keep the pipeline alive for demos.
4. Spark reads both topics, scores news sentiment with FinBERT, stores the latest news snapshot per symbol in Redis, and continuously produces enriched price-alert records.
5. Redis keeps the latest alert state for fast lookup.
6. ClickHouse stores historical alert records for analysis and reporting.

## Repository Structure

```text
crypto-realtime-alert/
├── infrastructure/
│   └── docker-compose.yml
├── data_ingestion/
│   ├── requirements.txt
│   └── src/
│       ├── config.py
│       ├── kafka_utils.py
│       ├── news_api_client.py
│       └── price_ws_client.py
├── stream_processing/
│   ├── requirements.txt
│   └── src/
│       ├── config.py
│       ├── nlp_analyzer.py
│       └── spark_pipeline.py
├── dashboard/
├── bot-tele/
├── run.py
└── README.md
```

## Key Features

- Environment-variable-based configuration with `.env`
- Stable Docker-based local infrastructure
- Real-time price ingestion from Binance
- News ingestion with CryptoPanic and RSS fallback
- FinBERT sentiment scoring
- Redis hot-state storage
- ClickHouse historical storage
- Single-command orchestration through `run.py`
- Localhost UIs for Kafka, Redis, and ClickHouse verification

## Tech Stack

- Python 3
- Apache Kafka
- Apache Spark Structured Streaming
- Redis
- ClickHouse
- Docker Compose
- Hugging Face Transformers
- FinBERT
- Streamlit for the future dashboard layer

## Getting Started

### 1. Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

### 2. Install dependencies

```bash
pip install -r data_ingestion/requirements.txt
pip install -r stream_processing/requirements.txt
```

### 3. Configure environment variables

Create your local `.env` file and provide values such as:

- `KAFKA_BROKER`
- `BINANCE_WS_URL`
- `CRYPTOPANIC_API_KEY`
- `CLICKHOUSE_HOST`
- `REDIS_HOST`

Do not commit `.env` to GitHub.

### 4. Start the full system

```bash
python run.py
```

This command:

- starts infrastructure
- ensures Kafka topics exist
- ensures the ClickHouse database and table exist
- starts the ingestion services
- starts the Spark pipeline

### 5. Check system health

```bash
python run.py status
```

You should see:

- infrastructure healthy
- managed Python services running
- data observed in Redis and ClickHouse

## Localhost Verification

After the system is running, you can inspect data from your browser:

- Kafka UI: `http://localhost:18080`
- RedisInsight: `http://localhost:5540`
- ClickHouse ping: `http://localhost:18123/ping`

Useful ClickHouse query example:

```text
http://localhost:18123/?user=crypto_admin&password=admin123&query=SELECT%20symbol,alert_status,price_change_pct,timestamp%20FROM%20crypto_alert.market_alerts%20ORDER%20BY%20processed_at%20DESC%20LIMIT%2010%20FORMAT%20JSON
```

## Main Commands

```bash
python run.py
python run.py status
python run.py logs all --lines 40
python run.py stop
python run.py stop --with-infra
```

## Production Notes

- This repository is designed as a strong demo and portfolio-grade prototype.
- Redis is used as hot storage for current alert state, not long-term history.
- ClickHouse is the source of truth for historical alert records.
- The Telegram bot and dashboard layers are reserved for future extensions.

## Roadmap

- Build the Streamlit dashboard
- Add Telegram alert delivery
- Introduce an `alerts` Kafka topic for downstream consumers
- Add metrics, retry policies, and stronger observability

## License

This project is intended for learning, demo, and portfolio purposes unless you add a separate license file.
