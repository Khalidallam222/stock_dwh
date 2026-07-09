# 📊 Stock Market Analytics Data Warehouse

**A cloud-native data engineering pipeline that extracts live stock market data from the Alpha Vantage API, stages it in Amazon S3, transforms it with Python on EC2, and loads it into a PostgreSQL Star Schema on Amazon RDS — surfaced through a public Streamlit dashboard.**

[![Live Demo](https://img.shields.io/badge/Live%20Demo-Streamlit-FF4B4B?logo=streamlit&logoColor=white)](#-live-demo)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](#-tech-stack)
[![AWS](https://img.shields.io/badge/AWS-EC2%20%7C%20S3%20%7C%20RDS-232F3E?logo=amazonaws&logoColor=white)](#-architecture)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Star%20Schema-4169E1?logo=postgresql&logoColor=white)](#-star-schema)
[![Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=white)](#-architecture)

---

## 🔗 Live Demo

**Dashboard:** [app-stock-data-warehouse-khalid3llam.streamlit.app](https://app-stock-data-warehouse-khalid3llam.streamlit.app/) 

> ⚠️ **Note:** The dashboard is intentionally simple right now — it exists to demonstrate that the warehouse and pipeline work end-to-end. **More updates, charts, and features are coming soon.**

---

## 🎥 Video Demo

[![Watch the demo](https://img.shields.io/badge/▶-Watch%20the%20Demo-black?style=for-the-badge)](https://youtu.be/qMzHwVxzc-0)
---

## 🧱 Architecture


![Pipeline Architecture](docs/architecture-diagram.png)

```
Alpha Vantage API  (live stock prices + company data)
         ↓
      EC2 Python   (extract.py — pulls JSON from API)
         ↓
     S3 raw/       (raw JSON, partitioned by date)
         ↓
      EC2 Python   (transform.py — cleans and flattens)
         ↓
  S3 processed/    (clean CSV, ready to load)
         ↓
      EC2 Python   (load.py — maps foreign keys, inserts)
         ↓
  RDS PostgreSQL   (Star Schema Data Warehouse)
         ↓
   Apache Airflow  (daily orchestration, retries, monitoring)
         ↓
  Streamlit Dashboard (public analytics layer)
```

---

## ⭐ Star Schema

![Star Schema](docs/star-schema.png)


```
dim_date               fact_stock_prices          dim_company
─────────              ─────────────────          ───────────
date_id (PK) ◄──────── date_id (FK)               company_id (PK)
date                   company_id (FK) ──────────► symbol
year                   open_price                  company_name
month                  high_price                  sector
month_name             low_price                   industry
quarter                close_price                 exchange
quarter_name           volume                      country
day_of_week            price_change                currency
day_of_week_name       daily_range
is_weekend             loaded_at
```

---

## 🛠️ Tech Stack

`Python` · `PostgreSQL` · `Amazon RDS` · `Amazon S3` · `Amazon EC2` · `Apache Airflow` · `Streamlit` · `boto3` · `SQLAlchemy` · `pandas` · `Plotly` · `REST APIs` · `ETL` · `Data Warehousing` · `Star Schema`

---

## 📂 Project Structure

```
stock_dwh/
├── extract.py     # Pulls live prices from Alpha Vantage → S3 raw/
├── transform.py    # Cleans & flattens raw JSON → S3 processed/
├── load.py          # Loads processed data into RDS Star Schema
├── setup_rds.py                # Creates the Star Schema (DDL)
├── setup_dimensions.py   # One-time load of dim_date and dim_company
├── pipeline.py           # Orchestrates extract → transform → load
├── analysis.py            # Analytics queries against the warehouse
├── dags/
│   └── stock_dwh_pipeline.py   # Airflow DAG definition
├── dashboard/
│   ├── dashboard.py            # Streamlit dashboard app
│   └── requirements.txt
├── docs/
│   ├── architecture-diagram.png
│   └── demo.gif
└── .env                        # Local credentials (gitignored)
```

---

## ✨ Features

- **End-to-end ETL pipeline**: live API → data lake (S3) → data warehouse (RDS PostgreSQL)
- **Star Schema design** optimized for analytics queries (`dim_date`, `dim_company`, `fact_stock_prices`)
- **Orchestrated with Apache Airflow**: scheduled runs, automatic retries, run history and monitoring
- **Public Streamlit dashboard** connected via a least-privilege, read-only database role over an SSL-enforced connection
- **Security-conscious deployment**: credentials never committed to git, environment-specific secrets management (EC2 `.env` vs. Streamlit `secrets.toml`)

---

## 🚀 Getting Started

This repo includes a **complete A-to-Z guide** for anyone who wants to build this exact project from scratch — every AWS setup step, every script, and every concept explained along the way, with hands-on exercises to practice.

📖 **[Read the Full A-to-Z Build Guide](Stock%20Market%20Analytics%20Data%20Warehouse%20—%20A-to-Z%20Guide.md)**

The guide walks through:

1. Getting a free Alpha Vantage API key
2. Setting up S3, RDS, and IAM on AWS
3. Building the EC2 environment
4. Creating the Star Schema and dimension tables
5. Writing the extract → transform → load scripts
6. Orchestrating everything with Apache Airflow
7. Building and deploying the Streamlit dashboard

---

## 🗺️ Roadmap

- [ ] Expand the dashboard with more charts, filters, and historical trend views
- [ ] Add alerting on pipeline failure (Slack/email)
- [ ] Migrate to the Airflow TaskFlow API with XComs
- [ ] CI/CD for DAG deployment via GitHub Actions
- [ ] Explore zero-open-ports deployment with Cloudflare Tunnel

---

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

---

## 🙋 About

Built as a hands-on data engineering portfolio project covering the full lifecycle of a cloud data pipeline — from raw API ingestion to a production-style analytics warehouse and public dashboard.

Feel free to ⭐ the repo if you find the guide useful!