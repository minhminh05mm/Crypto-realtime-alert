# RUN.PY FLOW

## Muc dich

File [run.py](/home/minhminh05mm/crypto-realtime-alert/run.py) la entrypoint trung tam de van hanh toan bo he thong.

## Luong chay

1. User chay `python run.py` hoac `python run.py start`
2. `run.py` kiem tra `.env`, `docker-compose.yml`, `.venv`, `docker`, `docker compose`
3. `run.py` goi Docker Compose de khoi dong infrastructure
4. `run.py` doi `Kafka`, `Redis`, `ClickHouse` healthy
5. `run.py` dam bao 2 Kafka topics da ton tai:
   - `raw_prices`
   - `raw_news`
6. `run.py` dam bao ClickHouse database va bang alert da ton tai
7. `run.py` start 3 process nen:
   - `news_api_client`
   - `price_ws_client`
   - `spark_pipeline`
8. `run.py` luu PID vao `.runtime/processes.json`
9. `run.py` luu log tung process vao `.runtime/logs/`

## Cac lenh chinh

- `python run.py`
- `python run.py start`
- `python run.py status`
- `python run.py logs all --lines 40`
- `python run.py stop`
- `python run.py stop --with-infra`

## Y nghia trang thai

- `Infra running well: YES`
  Infrastructure dang chay tot.

- `Python services running well: YES`
  3 process Python dang song.

- `End-to-end data observed in Redis/ClickHouse: YES`
  Pipeline da co output that su, khong chi moi khoi dong service.
