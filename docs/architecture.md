
---

# ✅ FILE 2: `docs/architecture.md`

```markdown
# E-Commerce Data Pipeline Architecture

## Overview
This document explains the architecture, components, and design decisions of the e-commerce data analytics platform.

---

## System Components

### 1. Data Generation Layer
- Generates synthetic e-commerce data using Faker
- Outputs CSV files for customers, products, transactions, and items

---

### 2. Data Ingestion Layer
- Loads raw CSV data into PostgreSQL staging schema
- Batch ingestion using Python

---

### 3. Data Storage Layer

#### Staging Schema
- Exact CSV replica
- Minimal validation
- Temporary storage

#### Production Schema
- Fully normalized (3NF)
- Data cleansing applied
- Referential integrity enforced

#### Warehouse Schema
- Star schema
- Optimized for analytics
- Aggregate tables for performance

---

### 4. Data Processing Layer
- Data quality checks
- Cleansing & enrichment
- SCD Type-2 handling
- Aggregation logic

---

### 5. Data Serving Layer
- Analytical SQL queries
- Precomputed aggregates
- BI tool connectivity

---

### 6. Visualization Layer
- Power BI Desktop dashboards
- Interactive filtering & drill-downs

---

### 7. Orchestration Layer
- Pipeline orchestrator
- Scheduler for daily execution
- Monitoring & alerting

---

## Data Models

### Staging Model
- Raw data replica
- No transformations

### Production Model
- 3NF normalized
- Business rules enforced

### Warehouse Model
- Star schema
- Fact & dimension tables
- Aggregates for speed

---

## Technologies Used
- Python 3.11
- PostgreSQL
- Pandas
- SQLAlchemy
- Docker
- Power BI
- Pytest

---

## Deployment Architecture
- Dockerized PostgreSQL
- Dockerized Python services
- Local BI access
