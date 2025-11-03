# 🚢 GCP Data Engineering Project  
### Event-Driven Pipeline with Cloud Run, Airflow & BigQuery ML  

This project showcases how to build an **end-to-end data pipeline** on **Google Cloud Platform (GCP)** using an event-driven architecture.  
It integrates **Cloud Storage**, **Cloud Run Functions**, **Airflow (VM or Composer)**, and **BigQuery ML** to automate ingestion, transformation, monitoring, and machine learning — all in a cost-efficient, scalable way.

---

## 🧱 Architecture Overview  

| Component | Purpose |
|------------|----------|
| **Cloud Storage (GCS)** | Stores incoming CSV data files |
| **Cloud Run Functions** | Triggers Airflow DAGs when new files arrive |
| **Airflow (VM / Composer)** | Orchestrates the data flow between services |
| **BigQuery** | Acts as the main data warehouse |
| **Cloud Logging & Monitoring** | Tracks health, performance, and errors |
| **BigQuery ML** | Runs ML models directly using SQL |

---

## 🗃️ Dataset  

The project uses **IMF PortWatch Daily Port Activity** data (2019–2024), covering 1,600+ global ports.  
Each record includes date, port name, port calls, and import/export metrics.  

> For demonstration, the focus is on **Klaipeda Port (Lithuania)**.  
Files are divided yearly (e.g., `Daily_Port_Activity_Data_2021.csv`) to simulate incremental ingestion.

---

## ⚙️ Pipeline Setup  

### Option 1: Airflow on Compute Engine (VM)
1. Create a VM (e.g., `n2-standard-2`) with full Cloud API access.  
2. Install Python & Apache Airflow in a virtual environment.  
3. Configure BigQuery access using a service account key.  
4. Place DAG files under `~/airflow/dags`.  

**DAG Workflow**
- `list_gcs_objects`: Lists files in GCS.  
- `load_to_bigquery`: Loads CSVs into BigQuery.  
- `run_bq_query`: Filters data for “Klaipeda” and writes to a new table.  

### Option 2: Cloud Composer
Use the same DAG in Composer for a **managed Airflow** experience.  
No manual setup required — simply upload the DAG via the Composer UI.

---

## ⚡ Cloud Run Trigger  

A **Cloud Run function** listens for new files in GCS and triggers the Airflow DAG via API.

- **VM Airflow** → Uses Basic Auth (username/password).  
- **Composer** → Uses OAuth automatically with `google-auth`.

Each file upload automatically triggers the DAG → loads data → transforms it in BigQuery.

---

## 🔍 Logging & Monitoring  

- Uses **Cloud Logging** to capture messages like `Successfully triggered DAG`.  
- Creates **Log-based Metrics** to monitor successful triggers.  
- Configures **Alert Policies** (e.g., no DAG run in 30 minutes) → sends email/SMS alerts.  

This ensures the pipeline stays reliable and self-monitored.

---

## 🤖 BigQuery ML  

After ingestion, **BigQuery ML** is used to predict port activity (`portcalls`) for **June 2024** using past data (2019–2023).

### 1. Train Model  
```sql
CREATE OR REPLACE MODEL `dataset.portcalls_model`
OPTIONS(model_type='linear_reg', input_label_cols=['portcalls']) AS
SELECT year, month, day, portid, portname, country, portcalls
FROM `dataset.port_data`
WHERE month = 6 AND year BETWEEN 2019 AND 2023;

2. Predict on New Data

SELECT *
FROM ML.PREDICT(MODEL `dataset.portcalls_model`,
  (SELECT year, month, day, portid, portname, country, portcalls
   FROM `dataset.port_data`
   WHERE month = 6 AND year = 2024));

3. Feature Engineering (Improved Model)

Added:

total_portcalls_non_container

import_export_ratio

day_of_week

The new model achieved R² ≈ 0.81, a big jump from the earlier 0.06 — showing much better accuracy.

📊 Visualization

Used Looker Studio to visualize:

Actual vs Predicted portcalls

Improved accuracy after adding new features


🚀 Key Highlights

Serverless & event-driven: Cloud Run + GCS automate everything

Flexible orchestration: Airflow on VM or Composer

Built-in reliability: Cloud Logging & Monitoring for alerts

Simple ML: BigQuery ML lets you train models with just SQL


💡 Takeaways

This project demonstrates how to:

Build a production-style GCP data pipeline end-to-end

Integrate serverless triggers and Airflow orchestration

Monitor workflows with Logging & Alerts

Apply machine learning with SQL in BigQuery

A great example of how Data Engineers can leverage GCP-native tools to go from raw data → insights → predictions with minimal infrastructure management.


🧩 Tools Used

Cloud Storage · Cloud Run · Airflow / Composer · BigQuery · BigQuery ML · Cloud Logging · Cloud Monitoring · Looker Studio


🧑‍💻 Author

Kavya Samanthapudi
📍 Data Engineer | Cloud & ML Enthusiast
