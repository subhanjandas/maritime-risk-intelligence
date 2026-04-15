# Maritime Risk Intelligence: Geospatial Engineering at Scale
**Architecting an End-to-End Medallion Pipeline for Global Supply Chain Fragility**

## 📌 Executive Summary
This project demonstrates the engineering of a high-throughput geospatial data warehouse using **Google Cloud Platform (BigQuery)** and **Python**. By processing **8.5 Million+ rows** of raw NOAA AIS telemetry, the system identifies maritime risk anomalies and harbor congestion through a three-tier Medallion architecture.

## 📡 Data Lineage & Sourcing
A core challenge of this project was bridging the gap between high-volume historical archives and low-latency real-time streams. The system integrates two distinct data channels provided by **NOAA (National Oceanic and Atmospheric Administration)**.

### 🗄️ 1. Historical Backfill (The "Bulk" Engine)
* **Source:** [NOAA Office for Coastal Management](https://coast.noaa.gov/htdata/CMSP/AISData/)
* **Format:** High-density CSV archives (Zstandard compressed).
* **Scale:** Utilized a **1.5GB+ daily slice** containing approximately **8.5 million telemetry records**. 
* **Engineering Challenge:** This data is "cold" and frequently contains sensor noise or signal drift. I architected the **Bronze-to-Silver DML logic** specifically to handle the schema enforcement and coordinate normalization required to turn these massive archives into queryable assets.

### 🌐 2. Real-Time Telemetry (The "Live" Edge)
* **Source:** [NOAA AIS Search API (WebSockets)](https://www.navcen.uscg.gov/ais-search-api)
* **Protocol:** `wss://stream.ais.cloud.noaa.gov/v1/advisories`
* **Format:** Real-time JSON payloads.
* **Engineering Challenge:** Unlike batch data, the live stream is volatile and prone to "bursty" traffic. I developed the **Object-Oriented Ingestion Engine** (`src/ingestion.py`) to manage the WebSocket lifecycle, handle involuntary disconnections, and parse nested metadata pings as they arrive from the US Coast Guard’s terrestrial and satellite transponders.

#### **Why this matters for Supply Chain Risk:**
By combining these two sources, the pipeline isn't just a static analysis tool—it’s a framework capable of **Backtesting** (benchmarking against historical norms) and **Live Monitoring** (detecting immediate anomalies). This dual-path approach is critical for predicting harbor congestion and calculating "Time-to-Arrival" metrics with high statistical confidence.

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

## 📍 Geospatial Risk Visualization
To translate raw coordinates into actionable intelligence, I developed a risk-scoring dashboard using **BigQuery Geo Viz**. This interface serves as the final consumption layer for the **Gold Table** logic.

![Maritime Risk Dashboard](Maritime-risk-intelligence-dashboard.png)

### 🧩 Visualization Logic & Features
* **Spatial Materialization:** The map renders `ST_GEOGPOINT` objects materialized in the Silver layer, allowing for sub-second rendering of millions of historical pings.
* **Risk Categorization (The "Gold" Logic):**
    * **🔴 Critical (Red):** Vessels successfully intersecting the defined **Harbor Entry Polygon**. This indicates a high-probability docking event or chokepoint transition.
    * **🟡 Warning (Yellow):** Vessels within a 5km buffer of the port, identified via the `ST_DWITHIN` function, signaling an active approach.
* **Kinematic Filtering:** The dashboard allows for filtering by **Speed Over Ground (SOG)**, helping analysts distinguish between vessels at anchor versus those actively navigating.

#### **My Perspective:**

* **Prod:** In a production environment, this dashboard acts as a **Supply Chain Early Warning System**. By isolating the 11 unique vessels that entered the "Critical" zone from a pool of 8.5 million pings, we reduce the noise for logistics operators by **99.9%**, allowing them to focus exclusively on high-impact maritime events.
---
*Developed by Subhanjan Das*
