# InsightHub: Backend Analytics & BI Engine

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org)
[![Pandas](https://img.shields.io/badge/Pandas-2.0+-150458.svg?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-009688.svg?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Data Modeling](https://img.shields.io/badge/Data_Modeling-Dimensional-FF6F00.svg?style=for-the-badge)]()
[![Business Intelligence](https://img.shields.io/badge/Business_Intelligence-Dynamic-4CAF50.svg?style=for-the-badge)]()

## ⚡ TL;DR

- Backend analytics engine for job market insights
- Processes raw data → serves structured JSON APIs for dashboards
- Part of the larger InsightHub BI system (frontend not included)

---

## 📖 The Story: Bridging Messy Data and Fast Insights

Raw job market data is messy, complex, and computationally heavy to process on the fly. To solve this, I engineered this backend analytics engine as the foundational data layer for my graduation project, InsightHub. 

Instead of relying solely on out-of-the-box BI tools, I built a modular Python and FastAPI backend to act as a dedicated "heavy lifter." This module handles rigorous data wrangling, advanced pandas modeling, and dynamic metric calculations. By offloading complex aggregations to this centralized server, the downstream frontend remains highly interactive, scalable, and lightning-fast.

---

## 🏗️ System Architecture

This repository contains the **Analytics Department** module, which serves as the core intelligence engine for the InsightHub BI platform.


### Architecture Breakdown:

There are two main functions of this analytics system:

1.  **Offline Batch ETL:** Python scripts orchestrated by an Airflow Scheduler periodically ingest raw job market data from the **Adzuna Job Market API**. This data is cleaned, modeled, and stored in a specialized **Dimensional Model (Star Schema)** within the SQL Server Data Warehouse.
2.  **Real-Time API & BI Engine:** The **FastAPI Dynamic BI Engine** processes real-time requests for home dashboards and advanced exploration endpoints. It reads optimized data from the SQL Server Star Schema and serves structured **JSON Payloads** to the central **ASP.NET Core API Gateway** for final rendering on the user dashboard.

---

## 🧩 Where This Fits in the Full System

> **Note:** This repository contains **only the backend data engine**. 

* **The Input (Data Layer):** Ingests raw, unstructured job market data.
* **The Brain (This Repository):** An offline ETL pipeline cleans the data, while a real-time FastAPI engine computes dynamic KPIs, handles smart binning, and structures the output.
* **The Output (Frontend Layer - Not in Repo):** Delivers clean JSON payloads to a separate frontend client (designed for advanced Syncfusion visualizations like treemaps and geographic bubble maps) to render interactive dashboards.

---

## 💡 Business Questions This Module Enables

By processing complex queries instantly, this backend empowers business stakeholders to answer critical questions:
* *"When is the peak time for recruitment activity?"*
* *"What is the current seniority gap in the market?"*
* *"Which roles are currently dominating market demand?"*
* *"Who are the top market movers and spenders?"*

---

## ⚙️ Key Features (Business & Technical Value)

* **Automated Ingestion & Orchestration (`update.py` & `Requesting.py`)**
  * **Technical:** Implements a decoupled HTTP client with exponential backoff and distributed pagination. The orchestrator script manages the end-to-end flow from fetching to database loading.
  * **Business:** Ensures reliable, uninterrupted data collection without server timeouts, building a highly available data pipeline.
* **Advanced Data Transformation (`Cleaning.py`)**
  * **Technical:** Executes NLP (spaCy) for entity/skill extraction, rate-limited geocoding for regional mapping, and multi-currency conversions using historical exchange rates.
  * **Business:** Creates a reliable "Single Source of Truth," ensuring executives make decisions on accurate, standardized, and globally comparable data.
* **Optimized Storage & Caching (`Handling.py` & `Caching.py`)**
  * **Technical:** Manages local JSON serialization and implements a robust state management (Caching) system for NLP and geocoding outputs.
  * **Business:** Drastically reduces third-party API costs and processing time by preventing redundant computations on historical data.
* **Config-Driven BI Engine (`Configs.py`)**
  * **Technical:** Uses Python dictionaries and optimized lambda functions (leveraging the `:=` walrus operator) to calculate multi-dimensional metrics in a single memory pass.
  * **Business:** Reduces Time-to-Market. New KPIs or filters can be injected via config files without rewriting core application logic.
* **Dynamic Analytics & Smart Binning (`Analytics.py`)**
  * **Technical:** Features an algorithmic approach (`show_others`) to group "long-tail" data into an "Others" bucket, alongside safe dynamic expression evaluation (`pandas.eval`).
  * **Business:** Prevents visual clutter on frontend dashboards while retaining deep-dive capabilities and ensuring secure ad-hoc calculations.
* **Resilient Routing & Orchestration (`Services.py`, `Routes.py` & `main.py`)**
  * **Technical:** Isolates execution contexts for visual widgets in the service layer and routes standard API responses using Pydantic validation.
  * **Business:** Ensures high availability and a flawless user experience. If anomalous data crashes one chart, the rest of the dashboard continues to render perfectly.

---

## 🔌 API Output Example (JSON)

The engine serves highly structured, frontend-ready responses. Here is an example of a dynamic request for a Top Companies distribution chart:

```json
{
  "status": "success",
  "data": {
    "Total Jobs": {
      "data": 22154,
      "title": "Posted recently",
      "suffix": "Jobs",
      "type": "card"
    },
    "Seniority Level Distribution": {
      "data": [
        {
          "x": "Not Specified",
          "y": 18664
        },
        {
          "x": "Manager",
          "y": 1569
        },
        {
          "x": "Senior",
          "y": 897
        }
      ],
      "title": "Requested Seniority Distribution",
      "tooltip_format": "Level: {point.x}\nCount: {point.y} Jobs",
      "show_data_labels": true,
      "show_legend": true,
      "type": "doughnut"
    }
  }
}
```

---

## 🚀 How to Run

1. **Clone the repository (> Note: The `en_core_web_sm` model is required if you want to use `extract_entities_nlp()` method.):**
   ```bash
   git clone https://github.com/InsightHub-ASU-CS-Graduation-Project/Analytics-Department.git
   python -m spacy download en_core_web_sm
   ```
2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
3. **Run the ETL Pipeline (> Note: This step requires configuring your own data source API keys inside `update.py`):**
   ```bash
   python "Cleaning & Modeling/update.py"
   ```
4. **Start the FastAPI Server:**
   ```bash
   uvicorn API_Engine.main:app --reload
   ```

---

## 📂 Modular Project Structure

The codebase is organized by logical business functions, ensuring it is easy to maintain and scale:

```text
├── Cleaning & Modeling/
│   ├── Raw Data/               # Landing zone for immutable raw API responses
│   ├── Cache Data/
│   │   ├── Automated/          # Persistent state for NLP results, geocoding, and translations
│   │   └── Manual/             # Human-in-the-loop overrides and custom replacement logic
│   │
│   ├── Caching.py              # Low-latency state management for repetitive data operations
│   ├── Cleaning.py             # Core ETL engine: NLP-driven entity extraction and data normalization
│   ├── Handling.py             # Optimized File I/O handler for JSON serialization and lexicon builds
│   ├── Requesting.py           # Robust API client with automated retries and distributed pagination
│   └── update.py               # End-to-end pipeline orchestrator (Ingestion → Transformation → Persistence)
|
├── Analytics & Visualization/
│   ├── Analytics.py            # High-performance analytical engine for multi-dimensional aggregations
│   ├── Configs.py              # Dynamic, metadata-driven dashboard and KPI definitions
│   ├── Routes.py               # RESTful API endpoints with Pydantic-validated request/response cycles
│   ├── Services.py             # Resilient Service Layer for fault-tolerant widget resolution
│   └── main.py                 # ASGI application entry point and Uvicorn server configuration
|
├── Shared Data/                # Staging area for "Single Source of Truth" optimized datasets
├── .env                        # Environment variables and sensitive infrastructure credentials
└── (requirements.txt)          # Project dependencies and environment specifications
```

---

## 🌟 Why This Project Stands Out

This project demonstrates a shift from basic data reporting to **software engineering for analytics**. By separating the data processing logic from the visualization layer, I built an architecture that mirrors enterprise-grade BI systems. It proves an ability to write clean, scalable Python, optimize Pandas for memory efficiency, and build APIs that effectively bridge the gap between complex data and end-user business intelligence.
