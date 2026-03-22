# Tong Quan Du An Crypto Realtime Alert

## 1. Muc tieu du an

Du an nay xay dung mot he thong giam sat va canh bao som bien dong gia tien ao dua tren hai nguon du lieu:

- Gia crypto thoi gian thuc tu Binance WebSocket
- Tin tuc crypto tu CryptoPanic API

He thong sau do xu ly streaming bang PySpark, phan tich sentiment bang FinBERT, va ghi ket qua canh bao vao Redis va ClickHouse.

## 2. Kien truc tong the

He thong duoc to chuc theo monorepo voi 4 thanh phan chinh:

### Infrastructure

Quan ly ha tang local bang Docker Compose:

- Zookeeper
- Kafka
- Redis
- ClickHouse

### Data Ingestion

Thu thap du lieu raw tu cac nguon ngoai:

- Binance WebSocket cho gia
- CryptoPanic API cho tin tuc

Sau do day vao Kafka.

### Stream Processing

Xu ly nghiep vu chinh:

- Doc stream tu Kafka
- Parse schema
- Cham sentiment bang FinBERT
- Join stream gia va stream news theo window
- Sinh alert
- Ghi Redis va ClickHouse

### Dashboard

Thu muc du phong cho phase giao dien Streamlit.

## 3. Luong du lieu

```text
Binance WebSocket
    -> data_ingestion/src/price_ws_client.py
    -> Kafka topic raw_prices

CryptoPanic API
    -> data_ingestion/src/news_api_client.py
    -> Kafka topic raw_news

Kafka raw_prices + raw_news
    -> stream_processing/src/spark_pipeline.py
    -> Redis (hot data / current alerts)
    -> ClickHouse (cold data / history)
```

## 4. Cau truc thu muc

```text
crypto-realtime-alert/
├── infrastructure/
│   └── docker-compose.yml
├── data_ingestion/
│   ├── requirements.txt
│   └── src/
│       ├── __init__.py
│       ├── config.py
│       ├── kafka_utils.py
│       ├── news_api_client.py
│       └── price_ws_client.py
├── stream_processing/
│   ├── requirements.txt
│   └── src/
│       ├── __init__.py
│       ├── config.py
│       ├── nlp_analyzer.py
│       └── spark_pipeline.py
├── dashboard/
├── .env
├── Code chay file.txt
├── TONG_QUAN_DU_AN.txt
├── TONG_QUAN_DU_AN.md
└── HUONG_DAN_THEM_TELEGRAM_ALERT.txt
```

## 5. Mo ta vai tro tung thu muc

### `infrastructure/`

Chua cau hinh ha tang local. File chinh:

- `docker-compose.yml`: khoi dong Kafka, Zookeeper, Redis, ClickHouse va tao san topics Kafka.

### `data_ingestion/`

Chua toan bo logic lay du lieu tu ben ngoai vao Kafka.

- `requirements.txt`: dependency cua ingestion layer
- `src/config.py`: load bien moi truong va logging
- `src/kafka_utils.py`: helper tao Kafka producer
- `src/price_ws_client.py`: doc gia tu Binance WS va push vao `raw_prices`
- `src/news_api_client.py`: doc tin tu CryptoPanic va push vao `raw_news`

### `stream_processing/`

Chua logic xu ly nghiep vu trung tam.

- `requirements.txt`: dependency cua Spark, NLP, Redis, ClickHouse
- `src/config.py`: load config Spark va sink
- `src/nlp_analyzer.py`: singleton FinBERT analyzer
- `src/spark_pipeline.py`: Spark Structured Streaming job

### `dashboard/`

Thu muc du phong cho dashboard phase sau. Hien tai chua co implementation.

## 6. Mo ta vai tro tung file quan trong

### `.env`

Noi luu toan bo bien moi truong:

- Kafka
- Redis
- ClickHouse
- Spark
- FinBERT
- Threshold alert
- API URLs

### `Code chay file.txt`

File huong dan cac lenh de van hanh du an:

- tao venv
- cai dependency
- khoi dong Docker
- chay ingestion
- chay Spark pipeline
- kiem tra Redis / ClickHouse

### `data_ingestion/src/config.py`

Chiu trach nhiem:

- doc `.env`
- validate config
- setup logging dung cho ingestion

### `data_ingestion/src/kafka_utils.py`

Chiu trach nhiem:

- tao Kafka producer
- serialize JSON
- retry neu broker chua san sang

### `data_ingestion/src/price_ws_client.py`

Chiu trach nhiem:

- ket noi Binance WebSocket
- reconnect neu mat ket noi
- format schema `raw_prices`

### `data_ingestion/src/news_api_client.py`

Chiu trach nhiem:

- goi CryptoPanic API theo poll interval
- format schema `raw_news`
- tranh duplicate co ban theo `id`

### `stream_processing/src/config.py`

Chiu trach nhiem:

- load config Spark runtime
- load config Redis, ClickHouse
- load config FinBERT

### `stream_processing/src/nlp_analyzer.py`

Chiu trach nhiem:

- singleton wrapper cho FinBERT
- tranh load model lap lai
- fallback an toan ve `Neutral` neu inference fail

### `stream_processing/src/spark_pipeline.py`

Chiu trach nhiem:

- khoi tao SparkSession
- doc Kafka topics
- parse schema
- extract symbol tu title news
- tinh sentiment
- join 2 stream theo window
- tinh trang thai alert
- ghi Redis
- ghi ClickHouse

## 7. Su lien ket giua cac module

- `data_ingestion` ghi vao Kafka
- `stream_processing` doc Kafka va ghi ra Redis / ClickHouse
- `dashboard` se doc Redis / ClickHouse de hien thi trong phase sau
- `.env` la cau noi cau hinh chung giua tat ca module

## 8. Schema du lieu

### Kafka topic `raw_prices`

```json
{
  "symbol": "BTCUSDT",
  "price": 65000.50,
  "timestamp": 1710928492000
}
```

### Kafka topic `raw_news`

```json
{
  "title": "SEC approves ETF",
  "published_at": "2026-03-20T10:00:00Z"
}
```

## 9. Ket qua dau ra

### Redis

Luu alert hien tai theo tung symbol.

Vi du key:

```text
crypto:alerts:BTCUSDT
```

### ClickHouse

Luu lich su alert de phuc vu query, dashboard va bao cao.

Bang hien tai:

```text
crypto_alert.market_alerts
```

## 10. Dinh huong mo rong

Du an co the mo rong theo cac huong:

- Telegram bot gui alert cho user
- Dashboard Streamlit
- Observability va metrics
- Retry / DLQ / schema governance
- Deployment production tren cloud
