# infra/adf/main.tf (lub inne pliki w module adf)

resource "azurerm_data_factory" "adf_instance" {
  name                = var.data_factory_name
  location            = var.location
  resource_group_name = var.resource_group_name
}


# --- WAŻNE: Najpierw zdefiniuj potoki "podrzędne" ---

resource "azurerm_data_factory_pipeline" "bronze_ingestion_pipeline" {
  name                = "bronze-ingestion" # Nazwa zgodna z referencją w main-pipeline
  data_factory_id     = azurerm_data_factory.adf_instance.id
  folder              = "cv-demo1/bronze" # Opcjonalnie, jeśli chcesz inne foldery
  activities_json     = jsonencode([]) # Na razie może być pusty, ale musisz zdefiniować jego akcje
  # ... inne właściwości pipeline'u bronze-ingestion ...
}

resource "azurerm_data_factory_pipeline" "silver_process_pipeline" {
  name                = "silver-process" # Nazwa zgodna z referencją
  data_factory_id     = azurerm_data_factory.adf_instance.id
  folder              = "cv-demo1/silver"
  activities_json     = jsonencode([])
  # ... inne właściwości pipeline'u silver-process ...
}

resource "azurerm_data_factory_pipeline" "gold_presentation_pipeline" {
  name                = "gold-presentation" # Nazwa zgodna z referencją
  data_factory_id     = azurerm_data_factory.adf_instance.id
  folder              = "cv-demo1/gold"
  activities_json     = jsonencode([])
  # ... inne właściwości pipeline'u gold-presentation ...
}

# --- Teraz możesz zdefiniować główny potok, który się do nich odwołuje ---

resource "azurerm_data_factory_pipeline" "main_pipeline" {
  name                = "main-pipeline"
  data_factory_id     = azurerm_data_factory.adf_instance.id
  folder              = "cv-demo1"

  activities_json = jsonencode([
    {
      name = "bronze"
      type = "ExecutePipeline"
      typeProperties = {
        pipeline = {
          referenceName = azurerm_data_factory_pipeline.bronze_ingestion_pipeline.name # Referencja Terraformowa
          type          = "PipelineReference"
        }
        waitOnCompletion = true
      }
    },
    {
      name = "silver"
      type = "ExecutePipeline"
      dependsOn = [{
        activity             = "bronze"
        dependencyConditions = ["Succeeded"]
      }]
      typeProperties = {
        pipeline = {
          referenceName = azurerm_data_factory_pipeline.silver_process_pipeline.name # Referencja Terraformowa
          type          = "PipelineReference"
        }
        waitOnCompletion = true
      }
    },
    {
      name = "gold"
      type = "ExecutePipeline"
      dependsOn = [{
        activity             = "silver"
        dependencyConditions = ["Succeeded"]
      }]
      typeProperties = {
        pipeline = {
          referenceName = azurerm_data_factory_pipeline.gold_presentation_pipeline.name # Referencja Terraformowa
          type          = "PipelineReference"
        }
        waitOnCompletion = true
      }
    }
  ])
}