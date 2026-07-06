# 🚚 Real-Time Logistics Data Platform

A production-style end-to-end data engineering project that simulates real-time shipment events, processes them using Apache Spark Structured Streaming, stores data in a Delta Lake following the Medallion Architecture (Bronze → Silver → Gold), builds a Star Schema for analytics, orchestrates workflows with Apache Airflow, and loads curated data into Snowflake.

---

## Architecture

```text
Shipment Producer
        │
        ▼
Apache Kafka
        │
        ▼
Spark Structured Streaming
        │
        ▼
Bronze (Delta Lake)
        │
        ▼
Silver (Delta Lake)
        │
        ▼
Gold Star Schema (Delta Lake)
        │
        ▼
Analytics KPI Marts
        │
        ▼
Snowflake
```

---

## Tech Stack

- Python
- Apache Kafka
- PySpark Structured Streaming
- Delta Lake
- Apache Airflow
- Snowflake
- Docker
- PostgreSQL
- Git & GitHub

---

## Features

- Simulated real-time shipment event streaming
- Kafka-based event ingestion
- Spark Structured Streaming consumer
- Delta Lake Medallion Architecture
- Data validation and business transformations
- Star Schema dimensional modeling
- Gold KPI marts for analytics
- Airflow workflow orchestration
- Snowflake data warehouse integration

---

## Data Model

### Dimension Tables

- dim_driver
- dim_vehicle
- dim_warehouse
- dim_destination
- dim_status
- dim_date

### Fact Table

- fact_shipment_events

### KPI Marts

- warehouse_kpi
- driver_kpi
- vehicle_kpi
- status_kpi
- daily_kpi

---

## Project Structure

```text
.
├── airflow/
├── producer/
├── spark/
├── docker/
├── delta/
│   ├── bronze/
│   ├── silver/
│   └── gold/
└── README.md
```

---

## Workflow

```text
Producer
   ↓
Kafka
   ↓
Spark Streaming
   ↓
Bronze
   ↓
Airflow
   ↓
Silver
   ↓
Gold Star Schema
   ↓
KPI Marts
   ↓
Snowflake
```

---

## Future Improvements

- Incremental MERGE-based processing
- Data quality testing (Great Expectations)
- Power BI / Streamlit dashboards
- CI/CD with GitHub Actions
- Kubernetes deployment

---

## Author

**Muhammad Numan**
