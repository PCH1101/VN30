# VN30 Stock ETL Pipeline

> 📄 **Project documentation & detailed design:** [VN30_Project — Google Docs](https://docs.google.com/document/d/1km6v6u2EEHaTb5752AL_8OuRZesgcm5rtLw5DXheoNo/edit?tab=t.0)

A data collection (ETL) system for trading data of the **30 VN30 index stocks** (HOSE) from the **vnstock** API, processed using a Medallion architecture (Bronze → Silver → Gold) and visualized in **Grafana**.

---

## Overview

| Item | Detail |
|------|--------|
| Data source | vnstock (TCBS/SSI) |
| Scope | 30 VN30 stocks: ACB, BCM, BID, BVH, CTG, FPT, GAS, GVR, HDB, HPG, MBB, MSN, MWG, NLB, PDR, PLX, POW, SAB, SSI, STB, TCB, TPB, VCB, VHM, VIB, VIC, VNM, VPB, VRE, VJC |
| Data collected | Daily OHLCV, intraday order matches (ticks), 1-minute candles |
| Architecture | Medallion (Bronze → Silver → Gold), Galaxy Schema |

## System Architecture

```
[vnstock API] → [Airflow Worker] → [MinIO Bronze]
                                          ↓
                              [Polars Transform]
                                          ↓
                        [ClickHouse Silver: ticks + OHLCV]
                                          ↓ (Materialized View)
                        [ClickHouse Gold: 1-min candles + money flow]
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

- **Shark Alert (Real-time)** — Alerts on abnormal active buy/sell pressure when 1-minute volume is 3x the 20-minute average.
- **VN30 Dashboard** — Heatmap of the 30 stocks, Net Money Flow, and real-time 1-minute candles on Grafana.
- **VWAP Signal** — Buy/sell signal when price crosses the cumulative intraday VWAP line.

## Schedule

| DAG | Cron | Description |
|-----|------|-------------|
| `vn30_intraday_dag` | `*/2 9-11,13-15 * * 1-5` | Fetch tick data every 2 minutes (Mon-Fri) |
| `vn30_eod_dag` | `30 15 * * 1-5` | Finalize end-of-day OHLCV at 15:30 |

## Directory Structure

```
VN30/
├── dags/                    # Airflow DAGs (intraday + EOD)
├── docs/                    # Design docs & ETL flow
├── infra/
│   ├── clickhouse/          # Schema & seed data
│   └── grafana/             # Datasource & dashboards
├── scripts/                 # Backfill & one-off utilities
├── src/
│   ├── extractors/          # Data fetching (intraday, OHLCV)
│   ├── transformers/        # Polars transforms
│   ├── loaders/             # Bronze/Silver loader
│   ├── analytics/           # VWAP calculator
│   └── alerts/              # Telegram alert
├── tests/
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## Quick Start

See the detailed installation guide in **[docs/SETUP.md](docs/SETUP.md)**.

## Running Tests

```bash
pytest tests/ -v
```

## Stopping Services

```bash
docker compose down
```

---

*For the detailed design of the database, business problem, and ELT architecture, see the documentation: [VN30_Project](https://docs.google.com/document/d/1km6v6u2EEHaTb5752AL_8OuRZesgcm5rtLw5DXheoNo/edit?tab=t.0)*