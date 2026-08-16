# VN30 Stock ETL Pipeline

> 📄 **Tài liệu mô tả & thiết kế chi tiết dự án:** [VN30_Project — Google Docs](https://docs.google.com/document/d/1km6v6u2EEHaTb5752AL_8OuRZesgcm5rtLw5DXheoNo/edit?tab=t.0)

> 🌐 **Bản tiếng Anh:** [README.md](README.md)

Hệ thống thu thập (ETL) dữ liệu giao dịch của **30 mã cổ phiếu rổ VN30** (HOSE) từ API **vnstock**, xử lý theo kiến trúc Medallion (Bronze → Silver → Gold) và trực quan hóa trên **Grafana**.

---

## Tổng quan

| Mục | Chi tiết |
|-----|----------|
| Nguồn dữ liệu | vnstock (TCBS/SSI) |
| Phạm vi | 30 mã VN30: ACB, BCM, BID, BVH, CTG, FPT, GAS, GVR, HDB, HPG, MBB, MSN, MWG, NLB, PDR, PLX, POW, SAB, SSI, STB, TCB, TPB, VCB, VHM, VIB, VIC, VNM, VPB, VRE, VJC |
| Dữ liệu thu thập | OHLCV ngày, khớp lệnh intraday (ticks), nến 1 phút |
| Kiến trúc | Medallion (Bronze → Silver → Gold), Galaxy Schema |

## Kiến trúc hệ thống

```
[vnstock API] → [Airflow Worker] → [MinIO Bronze]
                                          ↓
                              [Polars Transform]
                                          ↓
                        [ClickHouse Silver: ticks + OHLCV]
                                          ↓ (Materialized View)
                        [ClickHouse Gold: nến 1 phút + money flow]
                                ↙               ↘
                    [Grafana Dashboard]   [Telegram Bot Alert]
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Source | vnstock (TCBS/SSI) |
| Language | Python 3.11+ (asyncio) |
| Transform | Polars |
| Orchestration | Apache Airflow 2.x |
| Bronze Storage | MinIO (S3-compatible) |
| Data Warehouse | ClickHouse |
| BI Dashboard | Grafana |
| Alert | Telegram Bot |
| Infrastructure | Docker Compose |

## Use Cases

- **Shark Alert (Real-time)** — Cảnh báo lực mua/bán chủ động đột biến khi khối lượng 1 phút gấp 3 lần trung bình 20 phút.
- **VN30 Dashboard** — Bản đồ nhiệt 30 mã, dòng tiền ròng (Net Money Flow), nến 1 phút realtime trên Grafana.
- **VWAP Signal** — Tín hiệu mua/bán khi giá cắt đường VWAP lũy kế trong ngày.

## Lịch chạy

| DAG | Cron | Mô tả |
|-----|------|-------|
| `vn30_intraday_dag` | `*/2 9-11,13-15 * * 1-5` | Fetch tick data mỗi 2 phút (T2-T6) |
| `vn30_eod_dag` | `30 15 * * 1-5` | Chốt OHLCV cuối ngày lúc 15:30 |

## Cấu trúc thư mục

```
VN30/
├── dags/                    # Airflow DAGs (intraday + EOD)
├── docs/                    # Tài liệu thiết kế & ETL flow
├── infra/
│   ├── clickhouse/          # Schema & seed data
│   └── grafana/             # Datasource & dashboards
├── scripts/                 # Backfill & one-off utilities
├── src/
│   ├── extractors/          # Fetch dữ liệu (intraday, OHLCV)
│   ├── transformers/        # Polars transforms
│   ├── loaders/             # Bronze/Silver loader
│   ├── analytics/           # VWAP calculator
│   └── alerts/              # Telegram alert
├── tests/
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## Bắt đầu nhanh

Xem hướng dẫn cài đặt chi tiết tại **[docs/SETUP.md](docs/SETUP.md)**.

## Chạy Tests

```bash
pytest tests/ -v
```

## Dừng services

```bash
docker compose down
```

---

*Thiết kế chi tiết về database, bài toán nghiệp vụ và kiến trúc ELT vui lòng xem tài liệu: [VN30_Project](https://docs.google.com/document/d/1km6v6u2EEHaTb5752AL_8OuRZesgcm5rtLw5DXheoNo/edit?tab=t.0)*