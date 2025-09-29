# Country Innovation Index

**Country Innovation Index** — an ETL / data lakehouse project that calculates and visualizes a country-level innovation score using publicly available data.

The project implements a layered Bronze → Silver → Gold (Medallion) architecture and is built to be modular, testable and cloud-deployable (Microsoft Azure).

---

## 🚀 Project summary

Project Overview & The Advanced Innovation Index
Goal: To design and calculate a proprietary Composite Innovation Index that ranks selected countries based on multiple publicly available socio-economic and scientific factors. The index is built using a robust methodology that includes normalization and dynamic weighting to handle data scarcity gracefully.

Advanced Index Calculation Methodology
Instead of a simple linear formula, the final Innovation Score is calculated as a dynamically weighted sum of four independent Sub-Indices. This approach provides a more nuanced and methodologically sound metric.

**Normalization**: All raw indicators are first transformed using the log(1+x) function to mitigate skewness from outliers (e.g., highly innovative microstates) and then scaled to a 0−1 range using **MinMaxScaler**.

**Sub-Indices**: Scaled indicators are aggregated into four key sub-indices using fixed weights (e.g., Patents Index combines patents_per_million, resident_patents_per_million, and patent_expansion_ratio).

**Dynamic Weighting**: If a country is missing data for an entire Sub-Index (e.g., research_index), that sub-index is excluded from the final calculation, and the weights of the remaining sub-indices are dynamically rescaled to sum up to 1.0, ensuring the final score remains fair and comparable.

| Sub-Index | Weight | Description | Key Indicators (Normalized) |
| :--- | :---: | :--- | :--- |
| **Patents Index** | 35% | Measures intellectual property output. | Total Patents, Resident Patents, Patent Expansion Ratio |
| **Research Index** | 35% | Focuses on human capital in science. | Researchers per Million, Nobel Laureates per Million |
| **R&D Index** | 20% | Measures investment in future growth. | R&D Expenditure % GDP |
| **Unemployment Index** | 10% | Captures labor market efficiency (Inverted). | Graduate Unemployment Rate |


<img width="529" height="102" alt="image" src="https://github.com/user-attachments/assets/81b0e726-495f-49b7-9699-04cdeb67e30e" />


---


## 🏗️ Architecture (high-level)

A concise, non-redundant description of the Medallion implementation used in this project. The goal here is clarity: each layer has a short purpose, the core components used and the artifact it produces.

**Bronze** — Raw ingestion
- **Purpose**: Capture raw payloads exactly as delivered by sources (API responses, uploaded files).
- **Core pieces**: Azure Durable Function (`IngestNow`) driven by manifests; ADLS Gen2 Bronze path; ADF trigger to start ingestion.
- **Artifact**: Raw files in `/bronze/{source}/{dataset}-{date}.json` and a small summary file `/bronze/outputs/ingestion_summaries/ingestion_summary__{correlation_id}.json`.
- **Signal**: Event Grid message containing the summary file URL.
  
**Silver** — Enriching & modeling
- **Purpose**: Clean, normalize, enrich and model data into Delta Tables for downstream consumption.
- **Core pieces**: PySpark notebooks (local for dev or Databricks for prod), reference tables, manual data (e.g. patents CSV).
- **Artifact**: Delta tables (e.g. `COUNTRY`, `NOBEL_LAUREATES`, `POPULATION`) and a processing summary `/silver/outputs/summaries/processing_summary__{correlation_id}.json`.
- **Signal**: Event Grid message with path to the processing summary.

**Gold** — Analytics & reporting
- **Purpose**: Produce business-ready datasets, apply final calculations, and expose them to BI tools.
- **Core pieces**: `FactInnovationBuilder` (implements normalization, sub-indices and weighting), Delta output, optional Synapse SQL view for Power BI.
- **Artifact**: `fact_innovation` (Delta table) and `gold/outputs/summaries/processing_summary__{correlation_id}.json` (validation + location).

| Area | Technologies | Functionality |
| :--- | :--- | :--- |
| **Orchestration** | Azure Data Factory, Azure Event Grid | Pipeline automation, event-driven layer communication. |
| **Processing** | Apache Spark (PySpark, local), Azure Durable Functions | MinMaxScaler, Log Transformation, complex index calculation. |
| **Data Warehouse** | Azure Synapse Analytics (Dedicated SQL Pool) | Final reporting layer for Power BI. |
| **Data Lakehouse** | Azure Data Lake Storage Gen2 (Delta Lake) | Layered storage (Bronze/Silver/Gold). |
| **Reporting** | Power BI | Visualization and business analysis. |
| **Automation** | Terraform, GitHub Actions | IaC (Infrastructure as Code) and CI/CD. |



A diagram is expected under `docs/architecture.png` if you add one.

---

## 🔁 Data flow & Summaries (clean, step-by-step)

This section describes the exact sequence from trigger → ingestion → final output, with emphasis on the summary JSON artifacts and traceability.

1. **Start** — ADF trigger (scheduled or manual) invokes the Bronze pipeline and generates a `correlation_id` GUID for the run.

2. **Manifest delivery** — Bronze pipeline reads a manifest from `bronze/manifest/dev.manifest.json` in Storage and forwards it (with `correlation_id`) to the Durable Function IngestNow.

3. **Ingestion** — `IngestNow` executes tasks listed in the manifest (APIs / file ingests), stores raw payloads in Bronze and produces an ingestion summary file located at:

`bronze/outputs/ingestion_summaries/ingestion_summary__{correlation_id}.json` The summary contains: `correlation_id`, `etl_layer`, `status`, `proccessed_items`, `results` with each item summary.

4. **Bronze → Event Grid** — The function publishes an Event Grid event that includes the storage URL to the ingestion summary.

5. **Silver consumption** — The Silver pipeline (listening to Event Grid) downloads the ingestion summary, resolves the Bronze files it needs and runs the PySpark notebook which builds Delta tables. When finished it writes:

`silver/outputs/summaries/processing_summary__{correlation_id}.json` The summary contains: `correlation_id`, `etl_layer`, `status`, `proccessed_models` and `results` with each model summary.

6. **Silver → Event Grid** — Silver publishes the processing summary to Event Grid so Gold can start.

7. **Gold processing & publish** — Gold runs `FactInnovationBuilder`, computes `fact_innovation`, writes the Delta output and emits:

`gold/outputs/summaries/gold_summary__{correlation_id}.json` The gold summary contains final table location(s), validation metrics (row counts, key distribution checks) and an overall run status.

8. **Traceability & idempotency** — Every artifact (manifests, summary files, created tables) embeds the `correlation_id`. Durable Functions and orchestrators use idempotency keys to ensure retries do not produce duplicates.

All pipeline executions are correlated by a `correlation_id` GUID for traceability.

---

## 📄 Example manifests

Bronze manifest (`bronze/manifest/dev.manifest.json`):

```json
{
  "env": "dev",
  "etl_layer": "bronze",
  "sources": [
    {
      "source_config_payload": {
        "domain_source_type": "api",
        "domain_source": "NOBELPRIZE",
        "dataset_name": "laureates",
        "request_payload": { "offset": 0, "limit": 100 }
      }
    },
    {
      "source_config_payload": {
        "domain_source_type": "api",
        "domain_source": "WORLDBANK",
        "dataset_name": "population",
        "request_payload": { "indicator": "SP.POP.TOTL", "format": "json", "per_page": 1000 }
      }
    }
    // ... other sources
  ]
}
```

Silver manifest (`silver/manifest/dev.manifest.json`):

```json
{
  "env": "dev",
  "etl_layer": "silver",
  "references_tables": {
    "country_codes": "/references/country_codes/country-codes.csv"
  },
  "manual_data_paths": [
    { "domain_source": "PATENTS", "dataset_name": "patents", "file_path": "/manual/patents/patents.csv" }
  ],
  "models": [
    { "model_name": "COUNTRY", "source_datasets": [ { "domain_source": "NOBELPRIZE", "dataset_name": "laureates" }, { "domain_source": "WORLDBANK", "dataset_name": "population" }, { "domain_source": "PATENTS", "dataset_name": "patents" } ] },
    { "model_name": "NOBEL_LAUREATES", "source_datasets": [ { "domain_source": "NOBELPRIZE", "dataset_name": "laureates" } ], "depends_on": ["COUNTRY"] }
    // ... other models
  ]
}
```

Gold manifest (`gold/manifest/dev.manifest.json`):

```json
{
  "env": "dev",
  "etl_layer": "gold",
  "dims": [
    {
      "name": "dim_country",
      "source_models": ["COUNTRY"],
      "primary_keys": ["country_code"]
    },
    {
      "name": "dim_year",
      "source_models": ["YEAR"],
      "primary_keys": ["year"]
    }
  ],
  "facts": [
    {
      "name": "fact_innovation",
      "source_models": ["COUNTRY", "POPULATION", "GRADUATE_UNEMPLOYMENT", "RESEARCHERS", "PKB", "RD_EXPENDITURE", "PATENTS", "NOBEL_LAUREATES", "YEAR"],
      "primary_keys": ["country_code"]
    }
  ]
}
```
---

## 🧾 Example summary messages

After Bronze completes, the Durable Function emits `ingestion_summary__` (example):

```json
{
    "status": "COMPLETED",
    "env": "dev",
    "etl_layer": "bronze",
    "correlation_id": "07fa9c40-5d30-421f-8614-24a63b4d19f9",
    "timestamp": "2025-09-20T00:01:37.543092",
    "processed_items": 6,
    "duration_in_ms": 33400,
    "results": [
        {
            "status": "COMPLETED",
            "correlation_id": "07fa9c40-5d30-421f-8614-24a63b4d19f9",
            "duration_in_ms": 2441,
            "record_count": 1004,
            "domain_source": "NOBELPRIZE",
            "domain_source_type": "api",
            "dataset_name": "laureates",
            "message": "API data successfully processed. Uploaded 1 file with 1004 records.",
            "output_paths": [
                "https://demosurdevdatalake4418sa.blob.core.windows.net/bronze/nobelprize/2025/09/20/laureates_07fa9c40-5d30-421f-8614-24a63b4d19f9_d2b028b2.json"
            ],
            "start_time": "2025-09-20T00:01:20.303335",
            "end_time": null,
            "error_details": {}
        },
        {
```

Silver's `processing_summary__.json` may look like:

```json
{
    "status": "COMPLETED",
    "env": "dev",
    "etl_layer": "silver",
    "correlation_id": "37037a0e-72d8-49d8-93bb-ca9a84b69f5b",
    "timestamp": "2025-09-17T09:29:18.173977",
    "processed_models": 9,
    "duration_in_ms": 651754,
    "results": [
        {
            "model": "COUNTRY",
            "status": "COMPLETED",
            "output_path": "abfss://silver@demosurdevdatalake4418sa.dfs.core.windows.net/COUNTRY",
            "correlation_id": "37037a0e-72d8-49d8-93bb-ca9a84b69f5b",
            "operation_type": "UPSERT",
            "record_count": 249,
            "error_details": {},
            "duration_in_ms": 39817
        },
```


Gold publishes `gold_summary.json` with the location of `fact_innovation`.

```json
{
    "status": "COMPLETED",
    "env": "dev",
    "etl_layer": "gold",
    "correlation_id": "37037a0e-72d8-49d8-93bb-ca9a84b69f5b",
    "processed_models": 3,
    "duration_in_ms": 207365,
    "results": [
        {
            "model": "dim_country",
            "status": "COMPLETED",
            "output_path": "abfss://gold@demosurdevdatalake4418sa.dfs.core.windows.net/dim_country",
            "correlation_id": "37037a0e-72d8-49d8-93bb-ca9a84b69f5b",
            "operation_type": "UPSERT",
            "record_count": 249,
            "error_details": {},
            "duration_in_ms": 45081
        },
```

---

## 📚 Data sources

* **Nobel Prize API** — `https://api.nobelprize.org/2.1/laureates`
* **World Bank API** — population, GDP, R&D, researchers, unemployment (endpoints used in manifests)
* **Patents** — manual CSV import (no public API used). We compute an additional column: `patent_expansion_ratio = abroad_patents / (resident_patents + 1)`.

---

## 📂 Repository structure (condensed)

Project root: `country-innovation-index/` — the primary project folder. Below is the actual structure used in the repo (with inline comments preserved):

```
country-innovation-index/
├── infra/terraform/ # Terraform for Azure resources
├── function_app.py # single Azure Function (temporarily at repo root) — TODO: move to src/bronze
├── src/
│    ├── common/                             # shared code (utilities, factory, registry)
│    ├── bronze/                             # bronze-layer functions & ingestion helpers
│    │   └── manifests/dev.manifest.json     # manifest uploaded by CI/CD (bronze manifest)
│    ├── silver/                             # silver transformations (notebooks, scripts)
│    ├── gold/                               # gold layer builders / reporting exports
│    ├── azure_data_factory/                 # ADF JSON definitions synced from the ADF service
│    └── synapse/                            # Synapse artifacts exported from workspace
├── tests/                                   # unit / integration tests
├── .github/workflows/main.yml               # CI/CD workflow (GitHub Actions)
└── README.md
```

---

## 🧠 Code patterns & implementation notes

**Software Design Patterns (Python)**
The codebase uses patterns to enhance flexibility, readability, and testability:

**Factory Pattern:**
Used to dynamically create class instances based on the ETL layer (Bronze/Silver/Gold).

The base class BaseFactoryFromRegistry and the specific OrchestratorFactory automatically return the correct BaseOrchestrator object for a given layer.

**Orchestrator Pattern:**
Every major operation in a layer (e.g., BronzeOrchestrator) inherits from BaseOrchestrator, centralizing execution logic and managing the correlation_id for consistent end-to-end tracking.

**Configuration and Tracking**
Centralization: Configuration is managed via Azure App Configuration, enabling a single source of truth for both Azure Functions (Python) and Spark transformations (PySpark).

**Security**: Sensitive data (API keys) is stored securely in Azure Key Vault and referenced through App Configuration.

**Traceability**: All operations are linked by a unique correlation_id, passed between layers (Bronze → Silver → Gold) via Event Grid, allowing for full auditing of the entire data flow.

---

## ▶️ Quick run (short)

> Note: a full run requires an Azure subscription with permissions to create ADLS, ADF, Functions and Key Vault.

1. Clone the repo:

```bash
git clone https://github.com/LukaszBarbarian/country-innovation-index.git
cd country-innovation-index
```

2. Edit Terraform variables in `infra/terraform/` (subscription, resource group, location), then:

```bash
terraform init
terraform apply
```

3. Configure GitHub Actions secrets (Azure credentials, storage connection, Key Vault secrets).

4. Let the CI/CD pipeline deploy Functions + ADF (the workflow will push manifests and code).

5. Trigger Bronze via ADF trigger or call `IngestNow` locally for debugging.

6. Run Silver/Gold notebooks locally on Spark or configure Databricks.

---

## Diagrams & Reports

<img width="1086" height="621" alt="image" src="https://github.com/user-attachments/assets/93ed61d2-215b-461c-b03f-015a291b510b" />
<img width="1216" height="539" alt="image" src="https://github.com/user-attachments/assets/4ee7312a-ae49-4845-920c-6a55625ea7dd" />



---

## 📎 Repo link

[https://github.com/LukaszBarbarian/country-innovation-index](https://github.com/LukaszBarbarian/country-innovation-index)

---

## 👤 Author

**Łukasz Barbarian** — Data Engineer
https://www.linkedin.com/in/%C5%82ukasz-surowiec-594102161/

---

*End of README*
