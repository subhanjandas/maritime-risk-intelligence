# Maritime Risk Intelligence: Geospatial Engineering at Scale
**Architecting an End-to-End Medallion Pipeline for Global Supply Chain Fragility**

## 📌 Executive Summary
This project demonstrates the engineering of a high-throughput geospatial data warehouse using **Google Cloud Platform (BigQuery)** and **Python**. By processing **8.5 Million+ rows** of raw NOAA AIS telemetry, the system identifies maritime risk anomalies and harbor congestion through a three-tier Medallion architecture.

## 🏗️ System Architecture
The pipeline is designed for **scalability, idempotency, and low-latency analytics**, utilizing a decoupled ingestion and transformation strategy.

### 1. Ingestion Layer (Python/WebSockets)
* **Real-Time Streamer:** An Object-Oriented Python engine utilizing `asyncio` and `websockets` to poll global AIS advisories.
* **Batch Ingestion:** Optimized CLI-based loading of Zstandard-compressed archives (1.5GB+) into BigQuery Bronze.

### 2. The Medallion Pipeline (SQL/BigQuery GIS)
* **Bronze (Raw):** Schema-on-read landing zone for raw telemetry. 
* **Silver (Enriched):** Implemented **Partitioning (by date)** and **Clustering (by MMSI)**. Performed type-casting and spatial materialization using `ST_GEOGPOINT`.
* **Gold (Analytical):** Applied **Point-in-Polygon (ST_INTERSECTS)** spatial joins to categorize vessel risk based on proximity to critical harbor chokepoints (Hyannis Port Entrance).

## 🛠️ Technical Stack
* **Cloud:** Google Cloud Platform (BigQuery, GCS, Cloud Shell)
* **Languages:** Python 3.12, GoogleSQL (BigQuery GIS)
* **Data Science:** Pandas, Scipy, Matplotlib, Seaborn
* **Quality Assurance:** Pytest (Unit testing for ingestion resilience)

## 📊 Analytical Insights (EDA)
The project includes a multidimensional Exploratory Data Analysis suite that goes beyond mapping:
* **Kinematic Profiling:** Isolated velocity anomalies using **Z-Score Outlier Detection**.
* **Temporal Rhythms:** Analyzed diurnal traffic patterns to identify peak-load windows for port infrastructure.
* **Correlation Analysis:** Investigated the non-linear relationship between vessel length and velocity profiles.

## 📁 Repository Structure
```text
.
├── src/                # OOP Ingestion & Processing Modules
├── sql/
│   ├── ddl/            # Schema Definitions (Partitioned/Clustered)
│   ├── dml/            # Transformation Logic (Bronze-to-Silver)
│   └── views/          # Business Logic Abstraction Layer
├── notebooks/          # Kinematic & Spatial EDA Assets
├── tests/              # Pytest Suite for Pipeline Robustness
└── requirements.txt    # Dependency Manifest
```

## 🚀 Key Results
* **Data Volume:** Processed 8,512,846 telemetry records.
* **Risk Identification:** Isolated **11 Critical Harbor Entries** and **5 Warning Approximations** from 8.5M pings.
* **Performance:** Optimized spatial queries to scan millions of records in sub-20 seconds via clustering.

---
*Developed by Subhanjan Das | Data Scientist & SME*
