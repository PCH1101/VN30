# Hướng dẫn cài đặt

Hướng dẫn cài đặt và chạy VN30 Stock ETL Pipeline từ đầu.

## Prerequisites

- **Docker Desktop** (hoặc Docker Engine + Compose plugin)
- **Python 3.11+**
- **Telegram Bot Token** (lấy từ [@BotFather](https://t.me/botfather))

## 1. Clone và cấu hình environment

```bash
# Copy template
cp .env.example .env
```

Mở `.env` và điền các giá trị:

```env
# ClickHouse (có thể giữ default cho local dev)
CLICKHOUSE_PASSWORD=changeme

# MinIO
MINIO_ROOT_PASSWORD=minioadmin123

# Telegram (bắt buộc nếu muốn nhận alert)
TELEGRAM_TOKEN=your_bot_token_from_botfather

# Grafana
GRAFANA_ADMIN_PASSWORD=admin123

# Airflow
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
```

## 2. Khởi động services

```bash
docker compose up -d
```

Đợi tất cả services healthy (~60 giây):

```bash
docker compose ps
```

Kết quả mong đợi:

```
NAME                    STATUS
vn30_clickhouse         running (healthy)
vn30_minio              running (healthy)
vn30_postgres           running (healthy)
vn30_airflow_webserver  running (healthy)
vn30_airflow_scheduler  running
vn30_grafana            running (healthy)
```

## 3. Cài Python dependencies (Local development)

```bash
pip install -r requirements.txt
```

## 4. Kích hoạt Airflow DAGs

1. Truy cập Airflow Web UI: `http://localhost:8088` (Username: `admin`, Password: `admin`).
2. Gạt công tắc bật (Unpause) 2 DAGs:
   - `vn30_intraday_dag`
   - `vn30_eod_dag`

## Truy cập Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow Web UI** | http://localhost:8088 | admin / admin |
| **Grafana** | http://localhost:3000 | admin / (GRAFANA_ADMIN_PASSWORD) |
| **MinIO Console** | http://localhost:9002 | minioadmin / (MINIO_ROOT_PASSWORD) |
| **ClickHouse HTTP** | http://localhost:8123 | vn30_user / (CLICKHOUSE_PASSWORD) |

## Chạy Tests

```bash
# Unit tests (không cần Docker)
pytest tests/ -v

# Integration tests (cần Docker services đang chạy)
INTEGRATION=1 pytest tests/test_phase2.py -v -k "Integration"
```

Kết quả mong đợi:

```
63 passed, 4 skipped
```

## Dừng services

```bash
docker compose down

# Xóa cả data volumes (reset hoàn toàn)
docker compose down -v
```

## Troubleshooting

| Vấn đề | Giải pháp |
|--------|-----------|
| ClickHouse không healthy | Tăng `start_period` trong compose, check log: `docker logs vn30_clickhouse` |
| Telegram không nhận được | Kiểm tra `TELEGRAM_TOKEN` và `TELEGRAM_CHAT_ID` trong `.env` |
| MinIO bucket không tồn tại | Chạy lại `minio-init` container: `docker compose run minio-init` |
| Grafana không có data | Đảm bảo ClickHouse datasource kết nối: Grafana → Connections → Test |
| Import error vnstock3 | `pip install vnstock3` — cần internet để cài package |