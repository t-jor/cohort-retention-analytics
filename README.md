# Cohort Analysis Pipeline – BigQuery → Fivetran → Databricks

## 📌 Executive Summary

Cohort analysis is a key method in e-commerce analytics to understand how customer behavior evolves over time.  
It helps answer essential business questions:

- **How effectively are we acquiring new customers?**  
- **Do first-time buyers return — and how quickly?**  
- **Which monthly cohorts perform better or worse?**  
- **Is retention improving or declining over time?**

This project simulates a real-world analytics pipeline for a fictitious online shop (2024).  
Although the Databricks catalog uses the common term *ETL*, the workflow itself follows a modern **ELT pattern** (data is loaded first via Fivetran and transformed entirely inside Databricks).  
Using cloud-native ELT tools (**BigQuery → Fivetran → Databricks**), the pipeline produces insights into:

- Monthly cohort sizes  
- Retention after 1, 2, and 3 months  
- Depth of repeat purchasing  
- Contribution of new vs. existing customers over time  

The entire workflow follows the **Medallion Architecture** (Bronze → Silver → Gold → Serve Layer) and ends in an **interactive Databricks Lakeview dashboard**.

---

## 🚀 Architecture Overview

```text
BigQuery (Source)
        │
        ▼
Fivetran (Managed Ingestion)
        │
        ▼
Databricks Lakehouse
 ├── Bronze  – Raw Fivetran tables
 ├── Silver  – Cleaned facts (orders & customers)
 ├── Gold    – Cohort analytics tables
 └── Serve   – Lakeview Dashboard
```

---

## 1. Data Source – BigQuery

The raw dataset `ecom_orders` contains ~1,000 transactions from a fictitious e-commerce store for the year 2024.  
It serves as the single source of truth for this ELT pipeline.

### ✔ BigQuery Dataset

![BigQuery Source](img/source_bigquery.png)

---

## 2. Ingestion Layer – Fivetran

A **Fivetran BigQuery → Databricks** connector performs automated ingestion every 6 hours.  
Schema changes and updates are handled seamlessly.

### Key properties

- Zero-maintenance ingestion  
- Automated sync on schedule  
- Schema evolution supported  
- Loaded into the Databricks catalog as Delta tables  

### ✔ Fivetran Connector

![Fivetran Connector](img/connector_fivetran.png)

---

## 3. Databricks Lakehouse – Medallion Architecture

All transformations are implemented using Databricks SQL & PySpark notebooks.

---

### 🥉 Bronze Layer – Raw Data

Contains the unmodified, synchronized table:

- `bronze_ecom_orders`

---

### 🥈 Silver Layer – Clean Business Facts

Two refined fact tables:

#### `silver_customer_facts`

- First purchase date  
- Cohort month  
- Second purchase date  
- Time-to-second-order (1/2/3 months)

#### `silver_order_facts`

- All orders enriched with:
  - new/existing customer status  
  - standardized dates  
  - order_month  

---

### 🥇 Gold Layer – Analytics Marts

Final analytical tables feeding the dashboard:

| Table                     | Description |
|--------------------------|-------------|
| `gold_cohort_sizes`      | Customers per cohort month |
| `gold_cohort_retention`  | 1-, 2-, and 3-month retention |
| `gold_repeat_purchases`  | Share reaching 2+, 3+, 4+ orders |
| `gold_order_distribution`| Monthly orders by new/existing customers |

---

## 📁 Databricks Catalog Structure

All tables are stored inside the Databricks lakehouse catalog.

### ✔ Databricks Catalog

![Databricks Catalog](img/databricks_catalog.png)

---

## 4. Pipeline Orchestration – Databricks Jobs

The full pipeline runs as a Databricks job with:

- Bronze → Silver → Gold → Dashboard dependency chain  
- Triggered by **Table Update** of `ecom_orders`  
- Serverless compute  
- Automated dashboard refresh  
- Email notifications  

### ✔ Successful DAG Run

![Databricks DAG](img/databricks_dag.png)

---

## 5. Serve Layer – Cohort Analysis Dashboard

The final dashboard highlights key e-commerce KPIs:

### 📊 Metrics

- Monthly order volume (new vs. existing customers)
- Cohort sizes
- Retention after 1/2/3 months
- Repeat purchase depth (2+, 3+, 4+ orders)

### 🔍 Insights (2024)

- **Acquisition declines** sharply from mid-year  
- **1-month retention drops** after May  
- **Repeat purchase depth weakens**, suggesting fewer loyal heavy users  
- Overall → **activation and retention effectiveness is decreasing**

### ✔ Dashboard Screenshot

![Dashboard](img/cohort_dashboard.png)

---

## 6. Repository Structure

```text
ETL-Project-Cohort-Analysis/
│
├── notebooks/
│   ├── 01_bronze/
│   ├── 02_silver/
│   ├── 03_gold/
│   ├── 04_views/
│   └── 90_inspect/
│
├── img/
│   ├── source_bigquery.png
│   ├── connector_fivetran.png
│   ├── databricks_catalog.png
│   ├── databricks_dag.png
│   └── cohort_dashboard.png
│
├── docs/        # Optional additional screenshots
├── LICENSE
└── README.md
```

---

## 7. Technologies Used

- **Google BigQuery** – source system  
- **Fivetran** – managed ingestion  
- **Databricks Lakehouse** – Delta, SQL, PySpark, Jobs, Lakeview  
- **GitHub** – version control  

---

## 8. How to Reproduce

1. Create BigQuery dataset & load `ecom_orders`  
2. Configure Fivetran BigQuery → Databricks connector  
3. Create Databricks catalog & schema  
4. Run Bronze, Silver, Gold notebooks  
5. Build Lakeview Dashboard  
6. Configure Databricks Job Pipeline  
7. Validate using the inspection notebook  

---

## 9. Next Steps (Realistic Extension Options)

Given the limited sample dataset, meaningful improvements include:

- Add product, channel, or demographic dimensions  
- Introduce dbt for modular modeling & testing  
- Add data quality expectations  
- Publish dashboard externally (Tableau / Power BI)  
- Parameterize the pipeline for multiple datasets  

---

## 👤 Author

Thomas Jortzig — ELT Pipeline & Visualization for Cohort Analysis (11/2025)
