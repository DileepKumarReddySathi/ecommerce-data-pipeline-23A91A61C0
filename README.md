# E-Commerce Data Pipeline & Analytics Platform
![CI](https://github.com/<username>/ecommerce-data-pipeline-23A91A61C0/actions/workflows/ci.yml/badge.svg)
![Coverage](https://img.shields.io/badge/coverage-70%25-green)


## Project Overview
This project implements a complete end-to-end E-Commerce Data Pipeline that generates synthetic data, processes it through multiple database layers, applies analytics, and visualizes insights using BI dashboards.

The pipeline follows industry best practices:
- Raw → Staging → Production → Warehouse architecture
- Automated scheduling
- Monitoring & alerting
- Data quality checks
- Unit testing
- BI dashboarding

---

## Project Architecture

### Data Flow Diagram

Raw CSV Data
↓
Staging Schema (Raw Load)
↓
Production Schema (Cleaned & Normalized - 3NF)
↓
Warehouse Schema (Star Schema)
↓
Analytics & Aggregations
↓
Power BI Dashboard

yaml
Copy code

---

## Technology Stack

| Layer | Technology |
|-----|-----------|
| Data Generation | Python (Faker) |
| Database | PostgreSQL |
| ETL | Python (Pandas, SQLAlchemy) |
| Orchestration | Python Scheduler |
| Monitoring | Python + SQL |
| BI Tool | Power BI Desktop |
| Containerization | Docker |
| Testing | Pytest |

---

## Project Structure

ecommerce-data-pipeline/
├── config/
│ └── config.yaml
├── data/
│ ├── raw/
│ ├── staging/
│ ├── processed/
├── dashboards/
│ ├── powerbi/
│ └── screenshots/
├── docs/
│ ├── architecture.md
│ └── dashboard_guide.md
├── logs/
├── scripts/
│ ├── data_generation/
│ ├── ingestion/
│ ├── transformation/
│ ├── monitoring/
│ ├── scheduler.py
│ └── pipeline_orchestrator.py
├── sql/
│ └── queries/
├── tests/
├── docker-compose.yml
├── pytest.ini
└── README.md

yaml
Copy code

---

## Setup Instructions

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- Power BI Desktop (Free)

### Setup
```bash
docker-compose up -d
Verify database:

bash
Copy code
docker ps
psql -h localhost -U postgres -d ecommerce_db
Running the Pipeline
Full Pipeline
bash
Copy code
python scripts/pipeline_orchestrator.py
Individual Steps
bash
Copy code
python scripts/data_generation/generate_data.py
python scripts/ingestion/ingest_to_staging.py
python scripts/transformation/staging_to_production.py
python scripts/transformation/load_warehouse.py
python scripts/transformation/generate_analytics.py
Running Tests
bash
Copy code
bash scripts/run_tests.sh
or

bash
Copy code
pytest tests/ -v
Dashboard Access
Power BI
File: dashboards/powerbi/ecommerce_analytics.pbix

Screenshots: dashboards/screenshots/

Database Schemas
Staging Schema
staging.customers

staging.products

staging.transactions

staging.transaction_items

Production Schema
production.customers

production.products

production.transactions

production.transaction_items

Warehouse Schema
warehouse.dim_customers

warehouse.dim_products

warehouse.dim_date

warehouse.dim_payment_method

warehouse.fact_sales

warehouse.agg_daily_sales

warehouse.agg_product_performance

warehouse.agg_customer_metrics

Key Insights
Electronics is the top-performing category

Revenue shows steady month-over-month growth

Top customers contribute majority of revenue

Weekend sales outperform weekdays

Digital payments dominate transactions

Challenges & Solutions
Challenge	Solution
Schema mismatch	Automated column validation
Data quality issues	Quality scoring checks
Timezone handling	UTC standardization
Pipeline failures	Retry & monitoring
Test reliability	Isolated fixtures

Future Enhancements
Real-time streaming (Kafka)

Cloud deployment

Machine learning predictions

Real-time alerting

Contact
Name: Dileep Reddy
Roll Number: 23A91A61C0
Email: 23a91a61c0@aec.edu.in