# 📈 VN30 Real-Time Stock Data Pipeline

> Pipeline thu thập, streaming, xử lý ELT và trực quan hóa dữ liệu thời gian thực cho 30 cổ phiếu rổ **VN30 (HOSE)** sử dụng **Kafka, ClickHouse, dbt, Airflow** và **Grafana**.

---

## 🏛️ Kiến Trúc (Architecture)

```
[vnstock API] ──(15s)──► [Apache Kafka] ──(Kafka Engine + MV)──► [ClickHouse OLAP]
                                                                        │
[Apache Airflow] ──(Daily/Micro-batch)──► [dbt Models] ───────────────┤
                                                                        ▼
                                                               [Grafana Dashboard]
```

---

## 🛠️ Tech Stack

| Thành phần | Công nghệ | Mục đích |
|---|---|---|
| **Ingestion** | Python 3.11 (`vnstock`, `aiohttp`) | Thu thập dữ liệu giao dịch khớp lệnh intraday ticks |
| **Streaming** | Apache Kafka 7.6 (KRaft) | Message buffer phân vùng theo mã cổ phiếu |
| **Data Warehouse** | ClickHouse 24.3 | Lưu trữ Time-Series OLAP & ingest trực tiếp qua Kafka Engine |
| **Transformation**| dbt-clickhouse 1.8 | Xử lý dữ liệu (Staging → Intermediate → Marts) |
| **Orchestration** | Apache Airflow 2.8 | Điều phối pipeline, trigger dbt & cảnh báo |
| **Visualization** | Grafana 10.3 | Dashboard theo dõi bảng giá & biểu đồ nến OHLCV 1 phút |

---

## 🚀 Khởi Động Nhanh (Quick Start)

### 1. Cấu hình môi trường
```bash
cp .env.example .env
```

### 2. Khởi chạy toàn bộ hệ thống
```bash
docker compose up -d
```

### 3. Kiểm tra trạng thái containers
```bash
docker compose ps
```

---

## 🌐 Danh Sách Dịch Vụ & Truy Cập

| Dịch Vụ | Địa chỉ Web / Port | Tài khoản mặc định | Mô tả |
|---|---|---|---|
| **Grafana** | [http://localhost:3000](http://localhost:3000) | `admin` / `admin123` | Dashboard trực quan hóa bảng giá & nến |
| **Airflow** | [http://localhost:8088](http://localhost:8088) | `admin` / `admin` | Quản lý & giám sát workflow |
| **Kafka UI** | [http://localhost:8090](http://localhost:8090) | *(Không cần đăng nhập)* | Quản trị Kafka topics & message stream |
| **ClickHouse HTTP** | [http://localhost:8123](http://localhost:8123) | `vn30_user` / `changeme` | REST API & Web query |
| **ClickHouse Native** | `localhost:9000` | `vn30_user` / `changeme` | TCP client & dbt connection |

---

## 📂 Cấu Trúc Dự Án

```
VN30/
├── dags/                  # Airflow DAGs
├── infra/                 # Cấu hình ClickHouse DDL & Grafana Provisioning
├── src/                   # Source code Python (Extractors & Kafka Producer)
├── transform/             # dbt project (Models: Staging, Intermediate, Marts)
├── docker-compose.yml     # Quản lý Docker services
└── requirements.txt       # Python dependencies
```

---

## ⚙️ Lệnh Thao Tác Thường Dùng

- **Chạy producer thủ công (ngoài Docker):**
  ```bash
  python3 -m src.producers.kafka_producer
  ```

- **Chạy dbt transform & test:**
  ```bash
  cd transform && dbt run --profiles-dir . && dbt test --profiles-dir .
  ```

- **Kiểm tra dữ liệu trong ClickHouse:**
  ```bash
  docker exec -it vn30_clickhouse clickhouse-client -u vn30_user --password changeme --database vn30 --query "
  SELECT 'raw_intraday_ticks' AS table, count() AS rows FROM raw_intraday_ticks
  UNION ALL
  SELECT 'mart_vn30_price_board' AS table, count() AS rows FROM mart_vn30_price_board;
  "
  ```

---

## 📄 License
MIT License.