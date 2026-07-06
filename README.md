# 🚚 Real-Time Logistics Data Platform

A production-style end-to-end data engineering project that simulates a real-time logistics platform.

The project ingests shipment events into Kafka, processes them using PySpark Structured Streaming, stores them in a Delta Lake using the Medallion Architecture (Bronze → Silver → Gold), builds a Star Schema for analytics, and orchestrates batch transformations using Apache Airflow.

---

# Architecture

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
                    Bronze Delta Layer
                             │
                  (Airflow Scheduled)
                             ▼
                    Silver Delta Layer
                             │
                  (Airflow Scheduled)
                             ▼
                  Gold Star Schema
                             │
                             ▼
                 Analytics Data Marts
```

---

# Technologies

| Technology | Purpose |
|------------|---------|
| Python | Producer & ETL |
| Apache Kafka | Real-time event streaming |
| PySpark Structured Streaming | Stream processing |
| Delta Lake | Lakehouse storage |
| Apache Airflow | Workflow orchestration |
| Docker | Containerization |
| PostgreSQL | Airflow metadata database |
| Git & GitHub | Version Control |

---

# Project Features

- Real-time shipment event simulation
- Kafka message streaming
- Spark Structured Streaming consumer
- Bronze, Silver and Gold Delta Lake architecture
- Data quality validation
- Business rule transformations
- Star Schema dimensional modeling
- KPI data marts
- Apache Airflow orchestration
- Dockerized infrastructure

---


## Delta Lake Implementation

This project uses Delta Lake as the storage layer for the Medallion Architecture.

Implemented features:

- Delta format storage
- Bronze / Silver / Gold architecture
- Streaming writes from Spark Structured Streaming
- Batch reads for downstream transformations
- ACID-compliant storage
- Checkpointing for reliable stream processing
- Schema enforcement using Spark

# Medallion Architecture

## Bronze

Raw immutable shipment events.

Example:

- Kafka metadata
- Raw JSON
- Ingestion timestamp

---

## Silver

Validated and standardized data.

Transformations include:

- Remove duplicates
- Validate required fields
- Convert timestamps
- Risk calculations
- Delivery progress tracking

---

## Gold

Business-ready analytical model.

### Dimension Tables

- dim_driver
- dim_vehicle
- dim_warehouse
- dim_destination

### Fact Table

- fact_shipment_events

---

# KPI Marts

The project builds analytical marts including:

- Warehouse KPIs
- Driver Performance
- Vehicle Health
- Shipment Status Summary

---

# Airflow DAG

The workflow orchestrates the following tasks:

```text
Bronze
    ↓
Silver
    ↓
Gold Star Schema
    ↓
Gold KPI Marts
```

---

# Shipment Lifecycle

Each shipment progresses through realistic logistics states.

```text
CREATED
      ↓
PICKED_UP
      ↓
IN_TRANSIT
      ↓
ARRIVED_AT_HUB
      ↓
OUT_FOR_DELIVERY
      ↓
DELIVERED
```

---

# Folder Structure

```text
.
├── airflow/
│   └── dags/
├── producer/
├── spark/
├── delta/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── docker/
├── requirements.txt
└── README.md
```

---

# Running the Project

## 1. Start infrastructure

```bash
docker compose up -d
```

---

## 2. Start Shipment Producer

```bash
python producer/shipment_producer.py
```

---

## 3. Start Spark Streaming Consumer

```bash
python spark/streaming_consumer.py
```

---

## 4. Trigger Airflow Pipeline

```text
Bronze
→ Silver
→ Gold
→ KPI Marts
```

---

# Future Improvements

- Snowflake Data Warehouse integration
- dbt transformations
- Streamlit / Power BI dashboard
- Data quality tests with Great Expectations
- CI/CD pipeline using GitHub Actions
- Kubernetes deployment
- Monitoring with Prometheus & Grafana

---

# Skills Demonstrated

- Data Engineering
- Streaming Data Processing
- Event-Driven Architecture
- ETL / ELT
- Medallion Architecture
- Star Schema Modeling
- Data Lakehouse
- Apache Spark
- Apache Kafka
- Apache Airflow
- Docker
- Python
- Delta Lake

---

# Author

Muhammad Numan
