# Hệ Thống Cảnh Báo Crypto Theo Thời Gian Thực

Đây là dự án Data Engineering theo hướng doanh nghiệp, được xây dựng để giám sát biến động giá tiền ảo và kết hợp sentiment từ tin tức thời gian thực để tạo cảnh báo sớm.

Hệ thống được tổ chức theo mô hình monorepo, gồm các lớp hạ tầng, thu thập dữ liệu, xử lý stream và dashboard. Nền tảng chính sử dụng Kafka, Redis, ClickHouse, PySpark và FinBERT.

## Kiến Trúc Tổng Thể

### Các thành phần hiện tại

- `infrastructure/`: chứa Docker Compose cho Zookeeper, Kafka, Redis, ClickHouse, Kafka UI và RedisInsight.
- `data_ingestion/`: các service Python lấy giá từ Binance WebSocket và lấy tin tức từ CryptoPanic hoặc RSS fallback, sau đó đẩy dữ liệu thô vào Kafka.
- `stream_processing/`: Spark Structured Streaming đọc dữ liệu từ Kafka, chấm điểm sentiment bằng FinBERT, enrich dữ liệu giá với news snapshot, ghi hot data vào Redis và lịch sử vào ClickHouse.
- `dashboard/`: để sẵn cho lớp hiển thị.
- `bot-tele/`: thư mục tạm thời cho tính năng gửi cảnh báo qua Telegram trong tương lai.

### Luồng dữ liệu

1. Binance WebSocket đẩy sự kiện giá vào topic `raw_prices`.
2. CryptoPanic đẩy sự kiện tin tức vào topic `raw_news`.
3. Nếu CryptoPanic bị rate limit, ingestion sẽ fallback sang RSS để pipeline vẫn chạy ổn định khi demo.
4. Spark đọc 2 topic, chấm điểm sentiment cho news, cập nhật snapshot tin tức mới nhất theo symbol vào Redis, sau đó tạo bản ghi alert enrich theo luồng giá.
5. Redis lưu trạng thái alert hiện tại để đọc nhanh.
6. ClickHouse lưu lịch sử alert phục vụ tra cứu và phân tích.

## Cấu Trúc Thư Mục

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

## Điểm Nổi Bật

- Cấu hình bằng biến môi trường qua `.env`
- Hạ tầng local ổn định bằng Docker
- Thu thập giá crypto theo thời gian thực từ Binance
- Thu thập tin tức với cơ chế fallback RSS
- Chấm điểm sentiment bằng FinBERT
- Redis lưu hot data
- ClickHouse lưu dữ liệu lịch sử
- Có thể khởi động toàn hệ thống bằng `run.py`
- Có localhost UI để kiểm tra Kafka, Redis và ClickHouse

## Công Nghệ Sử Dụng

- Python 3
- Apache Kafka
- Apache Spark Structured Streaming
- Redis
- ClickHouse
- Docker Compose
- Hugging Face Transformers
- FinBERT
- Streamlit cho dashboard trong tương lai

## Cách Chạy Nhanh

### 1. Tạo môi trường ảo

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

### 2. Cài thư viện

```bash
pip install -r data_ingestion/requirements.txt
pip install -r stream_processing/requirements.txt
```

### 3. Tạo file `.env`

Bạn cần cấu hình các biến như:

- `KAFKA_BROKER`
- `BINANCE_WS_URL`
- `CRYPTOPANIC_API_KEY`
- `CLICKHOUSE_HOST`
- `REDIS_HOST`

Không đưa `.env` lên GitHub.

### 4. Chạy toàn bộ hệ thống

```bash
python run.py
```

Lệnh này sẽ:

- khởi động hạ tầng
- đảm bảo topic Kafka đã tồn tại
- đảm bảo database và table ClickHouse đã tồn tại
- chạy data ingestion
- chạy Spark pipeline

### 5. Kiểm tra trạng thái

```bash
python run.py status
```

Nếu hệ thống ổn, bạn sẽ thấy:

- hạ tầng healthy
- các process Python đang chạy
- Redis và ClickHouse đã có dữ liệu

## Xem Dữ Liệu Bằng Localhost

Sau khi hệ thống đã chạy, bạn có thể xem trên browser:

- Kafka UI: `http://localhost:18080`
- RedisInsight: `http://localhost:5540`
- ClickHouse ping: `http://localhost:18123/ping`

Ví dụ query ClickHouse:

```text
http://localhost:18123/?user=crypto_admin&password=admin123&query=SELECT%20symbol,alert_status,price_change_pct,timestamp%20FROM%20crypto_alert.market_alerts%20ORDER%20BY%20processed_at%20DESC%20LIMIT%2010%20FORMAT%20JSON
```

## Các Lệnh Quan Trọng

```bash
python run.py
python run.py status
python run.py logs all --lines 40
python run.py stop
python run.py stop --with-infra
```

## Ghi Chú

- Repo hiện tại phù hợp để demo và portfolio.
- Redis là nơi lưu trạng thái hiện tại, không phải kho lịch sử.
- ClickHouse là nơi lưu lịch sử alert.
- `dashboard/` và `bot-tele/` là các thành phần dự kiến mở rộng tiếp theo.

## Hướng Phát Triển

- Xây dựng dashboard bằng Streamlit
- Thêm Telegram alert
- Thêm topic `alerts` để downstream consumer đọc dễ hơn
- Bổ sung metrics, retry và monitoring đầy đủ hơn
