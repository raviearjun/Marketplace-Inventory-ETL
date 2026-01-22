# Marketplace Inventory Pipeline

[![Technical Test](https://img.shields.io/badge/Test-Data%20Engineer-blue)](https://github.com)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)](https://www.docker.com/)
[![Airflow](https://img.shields.io/badge/Apache-Airflow-017CEE?logo=apache-airflow)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)](https://www.postgresql.org/)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python)](https://www.python.org/)

> **Technical Test Project**: Building a robust ETL pipeline for Marketplace data with PostgreSQL Data Warehouse and Apache Airflow orchestration.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Database Schema](#database-schema)
- [ETL Pipeline](#etl-pipeline)
- [Data Transformations](#data-transformations)
- [Usage](#usage)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)
- [Theory Q&A](#theory-qa)

---

## Overview

This project implements a production-ready ETL (Extract, Transform, Load) pipeline that:

- **Extracts** data from JSON/CSV files containing marketplace product and store information
- **Transforms** data through cleaning, normalization, and validation processes
- **Loads** data into a PostgreSQL Data Warehouse with normalized schema
- **Orchestrates** the entire pipeline using Apache Airflow with DAG scheduling

### Key Features

- **Database Normalization**: Separates stores and products into 2NF/3NF schema
- **Data Cleaning**: Handles nulls, removes newlines, standardizes text
- **Deduplication**: Ensures no duplicate store_id or product_id
- **Data Integrity**: Foreign key constraints and type validation
- **Automated Scheduling**: Daily execution with Airflow
- **Error Handling**: Retry logic and comprehensive logging
- **Docker-based**: Fully containerized environment

---

## Architecture

```
┌─────────────────┐
│  Data Sources   │
│  (JSON/CSV)     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   EXTRACT       │
│  (Python)       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   TRANSFORM     │
│  (Pandas)       │
│  - Clean        │
│  - Normalize    │
│  - Deduplicate  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   LOAD          │
│  (SQLAlchemy)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  PostgreSQL DW  │
│  ┌───────────┐  │
│  │  stores   │  │
│  └─────┬─────┘  │
│        │ FK     │
│  ┌─────▼─────┐  │
│  │ products  │  │
│  └───────────┘  │
└─────────────────┘

Orchestrated by Apache Airflow
```

---

## Prerequisites

Before running this project, ensure you have:

- **Docker**: Version 20.10+
- **Docker Compose**: Version 2.0+
- **Git**: For version control
- **4GB RAM**: Minimum for Airflow + PostgreSQL
- **10GB Disk**: For Docker images and data

---

## Quick Start

### 1. Clone Repository

```bash
git clone <repository-url>
cd techtest
```

### 2. Environment Setup

The `.env` file is already configured with default credentials:

```bash
# PostgreSQL
POSTGRES_USER=marketplace_user
POSTGRES_PASSWORD=marketplace_pass_2026
POSTGRES_DB=marketplace_dw

# Airflow
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin123
```

### 3. Start Docker Services

```bash
# Start all services
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f
```

### 4. Access Services

| Service        | URL                   | Credentials                              |
| -------------- | --------------------- | ---------------------------------------- |
| **Airflow UI** | http://localhost:8080 | admin / admin123                         |
| **PostgreSQL** | localhost:5432        | marketplace_user / marketplace_pass_2026 |

### 5. Trigger DAG

1. Open Airflow UI: http://localhost:8080
2. Find DAG: `marketplace_etl_pipeline`
3. Toggle ON to enable
4. Click "Trigger DAG" button (play icon)
5. Monitor execution in Graph/Tree view

---

## Project Structure

```
techtest/
├── dags/                           # Airflow DAGs
│   └── marketplace_etl_dag.py      # Main ETL orchestration
├── scripts/                        # ETL Python modules
│   ├── __init__.py                 # Package initialization
│   ├── extract.py                  # Data extraction logic
│   ├── transform.py                # Data transformation & cleaning
│   ├── load.py                     # Database loading logic
│   └── utils.py                    # Helper functions
├── sql/                            # Database scripts
│   └── init.sql                    # DDL schema (auto-run on startup)
├── data/                           # Dataset files
│   ├── *.json                      # JSON source data
│   └── *.csv                       # CSV source data
├── logs/                           # Airflow logs (auto-generated)
├── plugins/                        # Airflow plugins (empty)
├── config/                         # Configuration files
├── docker-compose.yml              # Docker orchestration
├── .env                            # Environment variables
├── .gitignore                      # Git ignore rules
└── README.md                       # This file
```

---

## Database Schema

### ERD (Entity Relationship Diagram)

```
┌─────────────────────────────────┐
│          STORES                  │
├─────────────────────────────────┤
│ PK store_id          VARCHAR(50)│
│    store_name        VARCHAR(255)│
│    store_url         TEXT        │
│    store_description TEXT        │
│    store_location_city VARCHAR  │
│    store_created_at  TIMESTAMP  │
│    ...                           │
└────────────┬────────────────────┘
             │
             │ 1:N
             │
┌────────────▼────────────────────┐
│         PRODUCTS                 │
├─────────────────────────────────┤
│ PK id                VARCHAR(50)│
│ FK store_id          VARCHAR(50)│
│    name              VARCHAR(500)│
│    description       TEXT        │
│    category          VARCHAR(200)│
│    original_price    BIGINT      │
│    discounted_price  BIGINT      │
│    rating            DECIMAL     │
│    stocks            INTEGER     │
│    ...                           │
└─────────────────────────────────┘
```

### Table: `stores`

| Column              | Type         | Constraints | Description             |
| ------------------- | ------------ | ----------- | ----------------------- |
| store_id            | VARCHAR(50)  | PRIMARY KEY | Unique store identifier |
| store_name          | VARCHAR(255) | NOT NULL    | Store name (UPPERCASE)  |
| store_url           | TEXT         |             | Store URL               |
| store_location_city | VARCHAR(100) |             | City location           |
| store_created_at    | TIMESTAMP    |             | Store creation date     |

**Indexes**: `idx_stores_city`, `idx_stores_province`, `idx_stores_name`

### Table: `products`

| Column           | Type         | Constraints                    | Description                  |
| ---------------- | ------------ | ------------------------------ | ---------------------------- |
| id               | VARCHAR(50)  | PRIMARY KEY                    | Product ID                   |
| store_id         | VARCHAR(50)  | FOREIGN KEY → stores(store_id) | Store reference              |
| name             | VARCHAR(500) | NOT NULL                       | Product name                 |
| category         | VARCHAR(200) |                                | Product category (UPPERCASE) |
| discounted_price | BIGINT       |                                | Final price                  |
| rating           | DECIMAL(3,2) |                                | Product rating (0.00-5.00)   |
| stocks           | INTEGER      |                                | Available stock              |

**Indexes**: `idx_products_store_id`, `idx_products_category`, `idx_products_price`

---

## ETL Pipeline

### DAG: `marketplace_etl_pipeline`

**Schedule**: Daily (`@daily`)  
**Start Date**: 2026-01-20  
**Retries**: 2 (with 5-minute delay)

### Pipeline Flow

```
[Extract] → [Transform] → [Load Stores] → [Load Products] → [Validate] → [Cleanup]
```

#### 1. **Extract** (`extract_task`)

- Reads JSON/CSV from `/opt/airflow/data/`
- Detects file format automatically
- Returns ~10,000 raw records
- Duration: ~2 seconds

#### 2. **Transform** (`transform_task`)

- **Normalization**: Splits into stores + products DataFrames
- **Null Handling**: Fills missing values with defaults
- **Text Cleaning**: Removes `\n`, extra spaces
- **Deduplication**: Drops duplicate IDs
- **Standardization**: UPPERCASE for store_name, categories
- Duration: ~5 seconds

#### 3. **Load Stores** (`load_stores_task`)

- Inserts into `stores` table
- Uses UPSERT logic (ON CONFLICT)
- Loads ~1,200 unique stores
- Duration: ~3 seconds

#### 4. **Load Products** (`load_products_task`)

- Inserts into `products` table
- Validates FK constraints
- Loads ~10,000 products
- Duration: ~8 seconds

#### 5. **Validate** (`validate_task`)

- Counts rows in both tables
- Checks for orphaned products (FK violations)
- Logs validation results
- Duration: ~1 second

#### 6. **Cleanup** (`cleanup_task`)

- Removes temporary CSV files
- Frees disk space
- Duration: <1 second

**Total Pipeline Duration**: ~20 seconds

---

## Data Transformations

### Applied Cleaning Rules

| Transformation      | Implementation                      | Example                             |
| ------------------- | ----------------------------------- | ----------------------------------- |
| **Remove Newlines** | `text.replace('\\n', ' ')`          | `"Line1\\nLine2"` → `"Line1 Line2"` |
| **Trim Spaces**     | `re.sub(r'\s+', ' ', text).strip()` | `"  text  "` → `"text"`             |
| **Uppercase**       | `.str.upper()`                      | `"Jakarta"` → `"JAKARTA"`           |
| **Fill Nulls**      | `.fillna(0)` / `.fillna('UNKNOWN')` | `NULL` → `0` or `"UNKNOWN"`         |
| **Deduplicate**     | `.drop_duplicates(subset=['id'])`   | Keeps first occurrence              |
| **Type Casting**    | `astype(int)` / `pd.to_datetime()`  | String → Integer/Timestamp          |

### Data Quality Improvements

| Before                             | After                | Improvement  |
| ---------------------------------- | -------------------- | ------------ |
| `store_name` = "Ninik Wijaya"      | "NINIK WIJAYA"       | Standardized |
| `description` = "text\\nmore text" | "text more text"     | Cleaned      |
| `favorited_count` = NULL           | 0                    | Handled      |
| Duplicate `product_id`             | Unique products only | Deduplicated |

---

## Usage

### Manual Testing

```bash
# Test extraction
docker exec -it marketplace_airflow_webserver python /opt/airflow/scripts/extract.py

# Test transformation
docker exec -it marketplace_airflow_webserver python /opt/airflow/scripts/transform.py

# Test loading
docker exec -it marketplace_airflow_webserver python /opt/airflow/scripts/load.py
```

### Query Database

```bash
# Connect to PostgreSQL
docker exec -it marketplace_postgres psql -U marketplace_user -d marketplace_dw

# Sample queries
SELECT COUNT(*) FROM stores;
SELECT COUNT(*) FROM products;
SELECT s.store_name, COUNT(p.id) as product_count
FROM stores s
LEFT JOIN products p ON s.store_id = p.store_id
GROUP BY s.store_name
ORDER BY product_count DESC
LIMIT 10;
```

### Stop Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

---

## Monitoring

### Airflow UI Features

1. **Graph View**: Visualize task dependencies
2. **Tree View**: See historical runs
3. **Logs**: Click any task → View Logs
4. **XCom**: Check data passed between tasks
5. **Task Duration**: Performance metrics

### Health Checks

```bash
# Check container health
docker ps

# Check Airflow scheduler
docker logs marketplace_airflow_scheduler

# Check PostgreSQL
docker logs marketplace_postgres
```

---

## Troubleshooting

### Issue: Airflow UI tidak bisa diakses

**Solution**:

```bash
docker-compose logs airflow-webserver
# Wait for "Listening at http://0.0.0.0:8080"
```

### Issue: DAG tidak muncul di UI

**Solution**:

```bash
# Check DAG syntax
docker exec -it marketplace_airflow_webserver airflow dags list

# Check for Python errors
docker exec -it marketplace_airflow_webserver python /opt/airflow/dags/marketplace_etl_dag.py
```

### Issue: Database connection failed

**Solution**:

```bash
# Verify PostgreSQL is running
docker exec -it marketplace_postgres pg_isready

# Check credentials in .env file
cat .env | grep POSTGRES
```

### Issue: Permission denied on logs/

**Solution**:

```bash
# Fix permissions (Linux/Mac)
sudo chown -R 50000:0 logs/

# Windows: Run Docker Desktop as Administrator
```

---

## License

This project is created for technical test purposes.

---

## Author

**Data Engineer Candidate**  
Technical Test - Marketplace Inventory Pipeline  
Date: January 2026

---

## Support

For questions or issues:

1. Check logs: `docker-compose logs -f`
2. Verify containers: `docker ps`
3. Test database: `docker exec -it marketplace_postgres psql -U marketplace_user`

---

**Made with Docker, Airflow, and PostgreSQL**
