# 💼 Loan Portfolio & Borrower Risk Insights

This project delivers a complete **Data Engineering and Business Intelligence** solution for analyzing loan portfolios and borrower behavior. It includes:

✅ Full **ETL orchestration** (CSV → PostgreSQL → Snowflake)
✅ Smart **SQL-based transformations** with dbt
✅ A dynamic **Power BI dashboard** with actionable financial risk insights

---

## 📊 Final Dashboard

![Loan Dashboard](/assests/dashbard.png)

> **Insights Covered:**

* Loan distribution by interest tier
* Borrower default rates
* Purpose-wise loan trends
* State-level borrower contribution
* Fragility score risk segmentation
* Income patterns by employment stability

---

## 🔧 Architecture Overview

| Layer             | Tool             | Role                                 |
| ----------------- | ---------------- | ------------------------------------ |
| 🗂 Ingestion      | Airflow + Python | Load CSV to PostgreSQL (staging)     |
| 🔁 Transfer       | Airflow + Python | Transfer data to Snowflake           |
| 🧮 Transformation | dbt              | SQL-based modeling in Snowflake      |
| 📈 BI & Insights  | Power BI         | Data visualization and business KPIs |

---

## 📦 Storage Layers

| Layer  | Platform   | Data                   |
| ------ | ---------- | ---------------------- |
| Bronze | PostgreSQL | Raw staging CSVs       |
| Silver | Snowflake  | Cleaned staging tables |
| Gold   | Snowflake  | Views built by dbt     |

---

## 🔄 ETL Orchestration

### Airflow DAGs:

* `file_ingestion_etl_dag.py`: Loads 4 CSVs into PostgreSQL in chunks.
* `pg_to_snowflake_dag.py`: Transfers PostgreSQL data into Snowflake schema `LOAN_ANALYTICS`.

---

## 🧠 Business Problem & Stakeholders

### Stakeholders:

* 💼 Business Executives
* 📊 Risk Analysts
* 🧑‍💻 Data Scientists

### Business Needs:

* Identify **borrower fragility and credit risk**
* Optimize **loan disbursement strategies**
* Enable **KPI tracking & real-time analytics**

---

## 🧮 dbt Transformations

| dbt Model                         | What It Does                                                                  | Business Insight                   |
| --------------------------------- | ----------------------------------------------------------------------------- | ---------------------------------- |
| `loan.sql`                        | Base table with cleaned fields                                                | Standardized raw loans             |
| `loan_analytics.sql`              | Adds buckets like interest tier, term category, app type                      | Flags patterns in risk             |
| `loan_applicant_profile.sql`      | Groups borrowers by income, utilization, employment                           | Identify borrower stability        |
| `loan_purpose_summary.sql`        | Aggregates loan counts, interest rates, default % by purpose                  | Optimize loan offerings            |
| `loan_risk_enriched.sql`          | Joins loans & borrowers, computes repayment stress & grade risk levels        | Feature-rich model-ready risk data |
| `statewise_borrower_behavior.sql` | Calculates avg income, DTI, utilization, inquiries by state                   | Understand regional borrower risk  |
| `borrower_profile_risk.sql`       | Assigns composite risk based on DTI, utilization, inquiries, derogatory marks | Segments borrowers by risk         |
| `credit_fragility_scoring.sql`    | Weighted score based on DTI, revolving, inquiries, etc. → fragility levels    | Score for credit vulnerability     |

---

## 📁 Folder Structure

```
├── dags/
│   ├── file_ingestion_etl_dag.py
│   └── pg_to_snowflake_dag.py
├── dbt_project/
│   ├── models/
│   │   ├── loan_analytics.sql
│   │   ├── loan_purpose_summary.sql
│   │   └── ...
│   ├── dbt_project.yml
│   └── profiles.yml
├── new_data/
│   ├── borrower_df.csv
│   ├── loan_df.csv
│   └── ...
├── powerbi/
│   └── dashboard.pbix
```

---

## 🚀 How to Run

### 1. Launch Docker Containers

```bash
docker-compose up --build
```

### 2. Load Data via Airflow

* Visit [http://localhost:8080](http://localhost:8080)
* Trigger:

  * `file_ingestion_etl_dag`
  * `pg_to_snowflake_dag`

### 3. Run dbt Models

```bash
cd dbt_project
dbt run
```

### 4. Open Power BI Dashboard

* Load views from `LOAN_ANALYTICS` schema
* Use filters (risk, geography, income band)

---

## 📈 Dashboard Visuals Breakdown

| Chart Title                             | Table Used                    | Visual Type     | Insight                            |
| --------------------------------------- | ----------------------------- | --------------- | ---------------------------------- |
| Loan Distribution by Interest Tier      | `loan_analytics`              | Pie Chart       | % of loans by rate category        |
| Borrower Status Breakdown               | `loan_analytics`              | Donut Chart     | % of current vs. defaulted loans   |
| Most Common Loan Purposes               | `loan_purpose_summary`        | Bar Chart       | Top purposes + their volume        |
| Financial Fragility Score by Risk Level | `credit_fragility_scoring`    | Bar Chart       | Fragility segmentation             |
| Statewise Borrower Base                 | `statewise_borrower_behavior` | Waterfall Chart | Contribution of each state         |
| Annual Income by Employment Stability   | `loan_applicant_profile`      | Line Chart      | How income varies by job stability |

---

## 📌 Conclusion

This end-to-end project demonstrates how to combine **modern data engineering** tools and **SQL modeling best practices** to drive **data-driven financial decisions**. From raw ingestion to executive-level dashboards, everything is automated and modular.

---

