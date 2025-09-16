# Country Innovation Index

**Country Innovation Index** is a data processing project designed to calculate and visualize the **Country Innovation Index** based on publicly available data. The project uses a **layered Bronze, Silver, Gold architecture** to transform raw data into ready-to-analyze datasets, ensuring modularity and scalability.

---

## 💡 The Country Innovation Index

The Country Innovation Index is a key indicator that reflects a nation's innovation potential. In this project, it is calculated using the following formula, combining data from various reliable sources:

$Innovation\_Index = (weight_1 * patents\_per\_million) + (weight_2 * Nobel\_laureates\_per\_million) + (weight_3 * R\&D\_expenditure\_\%GDP) + (weight_4 * researchers\_per\_million) - (weight_5 * graduate\_unemployment)$

---

## 🏗️ Project Architecture

The project is built on the **Medallion Architecture**, with each layer connected using **Azure Data Factory** and **Azure Event Grid**. The entire ETL (Extract, Transform, Load) process is orchestrated in Azure, ensuring reliability and automation.



### **Bronze Layer (Raw Data)**

* **Goal:** To ingest and store raw data.
* **Orchestration:** An **Azure Data Factory** pipeline is triggered on a schedule (e.g., weekly).
* **Data Ingestion:** The pipeline reads a **configuration manifest** (e.g., `manifest/dev.manifest.json`) from a Storage Account, which lists the data sources. This manifest is then passed to an **Azure Durable Function**, which asynchronously pulls data from the specified APIs.
* **Data Sources:**
    * **Nobel Laureates:** `https://api.nobelprize.org/`
    * **World Bank:** `https://api.worldbank.org/` (Population, GDP, R&D Expenditure, Researchers, Unemployment)
    * **Patents:** Manual, non-public API data.
* **Process:** Upon completion of the data ingestion, the Azure Function publishes a status message to **Azure Event Grid**, signaling that the next layer is ready to run.

### **Silver Layer (Enriched Data)**

* **Goal:** To clean, standardize, and model the data.
* **Orchestration:** The Silver pipeline listens for the Event Grid message and starts when it is received.
* **Transformation:** The pipeline runs a notebook in **Spark** (e.g., locally, to reduce Databricks costs), which:
    * Reads the Silver layer manifest (`manifest/silver.manifest.json`).
    * Joins data from different sources, including manual data (e.g., patents) and reference tables.
    * Creates and saves **data models in Delta Table format** (e.g., `dim_country`, `fact_laureates`), which are normalized and ready for further analysis.

### **Gold Layer (Analytics & Visualization Data)**

* **Goal:** To prepare data for direct consumption by business applications or visualizations.
* **Orchestration:** Similar to the Silver layer, the Gold pipeline is triggered by an Event Grid signal.
* **Transformation:** It performs the final calculations, such as the "per million" metrics (e.g., `patents_per_million`) and the final **Country Innovation Index**, by combining all processed data.
* **Result:** **Final fact and dimension tables** (e.g., `fact_innovation`) are created, which can be easily used to build dashboards or reports.

---

## 🛠️ Technology Stack

* **Orchestration:** Azure Data Factory, Azure Durable Functions
* **Data Storage:** Azure Data Lake Storage Gen2 (Delta Lake)
* **Data Processing:** Apache Spark
* **Configuration Management:** Azure App Configuration
* **Secret Management:** Azure Key Vault
* **Event-driven Communication:** Azure Event Grid
* **Infrastructure as Code:** Terraform
* **CI/CD:** GitHub Actions

---

## 📂 Repository Structure

The project's source code is organized in a modular and layered fashion, mirroring the system's architecture:

* `src/functions/`: Contains the Azure Functions code, divided into layers (`bronze`, `silver`, `gold`).
* `src/functions/common/`: Shared classes and functions used throughout the project, e.g., factories for class management.
* `src/functions/orchestrators/`: Orchestrator classes (`BaseOrchestrator`) that manage the main tasks within each layer.
* `manifests/`: Directory with configuration manifest files for each layer.
* `notebooks/`: Spark notebooks where the Silver and Gold layer transformations take place.
* `terraform/`: Terraform files for automated deployment of the Azure infrastructure.

### **Factories and Registries**

The project uses the **Factory Pattern** to dynamically create objects, which increases code flexibility and readability. An example is the `OrchestratorFactory` class (`src/functions/common/factory/base_factory_from_registry.py`), which returns the appropriate orchestrator object based on the provided ETL layer.

---

## 🚀 Running the Project

This project requires a **Microsoft Azure** environment to run. The infrastructure is fully defined in **Terraform** files. To run the project, you need to:

1.  Ensure you have access to an Azure subscription and have Terraform installed.
2.  Configure your Azure credentials for Terraform.
3.  Run the Terraform scripts to deploy all the necessary resources (Data Factory, Functions, Key Vault, etc.).
4.  Configure GitHub Actions to automatically deploy the code and manifests to the created Azure resources.

---

## 📈 Result Visualization

The final result of the project—the calculated Country Innovation Index for selected countries—is available in the Gold layer. This data can be used to create visualizations, for example, a bar chart that ranks countries by their innovation score.




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
