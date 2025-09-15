# 🌍 Country Innovation Index

This project demonstrates how to build a full data pipeline and analytics solution around the concept of **country-level innovation**.  
It covers ingestion, transformation, and visualization, following best practices for data lakehouse architecture (Bronze → Silver → Gold).

---

## 🚀 Project Overview
**Goal:** To design an **Innovation Index** that ranks countries based on multiple factors such as patents, Nobel laureates, R&D expenditure, and more.  

Innovation formula:

```text
InnovationIndex =
 (w1 * patents_per_million) +
 (w2 * nobel_laureates_per_million) +
 (w3 * rnd_expenditure_percent_gdp) +
 (w4 * researchers_per_million) -
 (w5 * graduate_unemployment_rate)
```

---

## 🏗 Architecture

![Architecture Diagram](docs/architecture.png)

**Ingestion (Bronze)**
- Azure Function (`IngestNow`) fetches data from APIs / files.  
- Azure Data Factory orchestrates data movement into the Bronze layer.  

**Transformation (Silver)**
- PySpark jobs clean, normalize, and standardize datasets.  
- Example: enrich patents with `patent_expansion_ratio = abroad_patents / (resident_patents + 1)`.  

**Analytics (Gold)**
- Final schema:  
  - `fact_innovation`  
  - `dim_country`  
  - `dim_year`  
- Optimized for reporting and dashboarding.  

**CI/CD & Infra**
- Terraform for infrastructure as code.  
- GitHub Actions for CI/CD pipelines.  

---

## 📸 Screenshots

Key visuals from the project:

- Azure Data Factory pipeline  
  ![ADF Screenshot](docs/adf_pipeline.png)

- Example GitHub Actions CI/CD run  
  ![CI/CD Screenshot](docs/cicd.png)

- Sample dashboard / chart  
  ![Chart Screenshot](docs/chart.png)

---

## 📊 Example Results

**Global Ranking (2023)**  
![Ranking Chart](docs/global_ranking.png)

**Country Timeline**  
Example: Poland innovation index trend with world events annotated.  
![Timeline Chart](docs/poland_timeline.png)

---

## 📦 Data Sources

- **Patents**: File-based dataset  
- **Nobel Laureates**: [Nobel Prize API](https://api.nobelprize.org/2.1/laureates)  
- **Population**: [World Bank](https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?format=json)  
- **GDP**: [World Bank](https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD?format=json)  
- **R&D Expenditure**: [World Bank](https://api.worldbank.org/v2/country/all/indicator/GB.XPD.RSDV.GD.ZS?format=json)  
- **Researchers**: [World Bank](https://api.worldbank.org/v2/country/all/indicator/SP.POP.SCIE.RD.P6?format=json)  
- **Unemployment**: [World Bank](https://api.worldbank.org/v2/country/all/indicator/SL.UEM.TOTL.ZS?format=json)  

---

## 📂 Repository Structure

```
cv-demo1/            # <- replace with your repo name if needed
│
├── infra/terraform/        # Infrastructure as code
├── src/functions/          # Azure Functions (IngestNow, etc.)
├── pipelines/adf/          # Azure Data Factory pipelines (JSON definitions)
├── notebooks/              # PySpark notebooks (Silver/Gold transformations)
├── data/                   # Example datasets (optional)
├── docs/                   # Diagrams, screenshots, charts
└── README.md
```

---

## ⚙️ How to Run Locally

1. Clone this repository:
   ```bash
   git clone https://github.com/<your-username>/cv-demo1.git
   cd cv-demo1
   ```

2. Deploy infrastructure via Terraform:
   ```bash
   terraform init
   terraform apply
   ```

3. Run ingestion (Azure Function or local trigger).  
4. Process transformations with PySpark (`notebooks/` folder).  
5. Explore results in `gold/fact_innovation`.  

---

## 🛠 Tech Stack

- **Cloud**: Azure (Functions, Data Factory, Storage)  
- **Processing**: PySpark (local), scalable to Databricks  
- **Orchestration**: GitHub Actions, Terraform  
- **Visualization**: Python (matplotlib/plotly) or BI tools (Power BI)  

---

## 📅 Future Improvements

- Add interactive dashboard (Dash/Streamlit/Power BI).  
- Automate Nobel Prize + patents data ingestion via scheduled pipelines.  
- Extend model with new indicators (education, startup ecosystem).  

---

## 👤 Author

**Your Name** – Data Engineer | [LinkedIn](TODO_link) | [Portfolio](TODO_link)
