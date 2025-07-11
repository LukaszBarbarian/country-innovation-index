# infra/adf/main.tf (lub inne pliki w module adf)

resource "azurerm_data_factory" "adf_instance" {
  name                = var.adf_name 
  location            = var.location
  resource_group_name = var.resource_group_name

  tags = {
    Environment = var.environment
    Project     = var.project_prefix
  }
}


# --- WAŻNE: Najpierw zdefiniuj potoki "podrzędne" ---
resource "azurerm_data_factory_pipeline" "bronze_ingestion_pipeline" {
  name            = "bronze-ingestion"
  data_factory_id = azurerm_data_factory.adf_instance.id
  folder          = "cv-demo1/bronze"

  # Definicja parametrów potoku
  parameters = {
  inputArray = jsonencode([
    {
      paramName  = "indicator"
      paramValue = "WHOSIS_000001"
    },
    {
      paramName  = "dimension"
      paramValue = "all"
    }
  ])
}
  # Definicja aktywności potoku (pozostaje bez zmian, jeśli została już naprawiona)
  activities_json = jsonencode([
    {
      "name": "who-foreach",
      "type": "ForEach", # Upewnij się, że to jest "ForEach", a nie ścieżka!
      "dependsOn": [],
      "userProperties": [],
      "typeProperties": {
        "items": {
          "value": "@pipeline().parameters.inputArray",
          "type": "Expression"
        },
        "isSequential": true,
        "activities": [
          {
            "name": "whoingest",
            "type": "AzureFunctionActivity",
            "dependsOn": [],
            "policy": {
              "timeout": "0.12:00:00",
              "retry": 0,
              "retryIntervalInSeconds": 30,
              "secureOutput": false,
              "secureInput": false
            },
            "userProperties": [],
            "typeProperties": {
              "functionName": "whoingest",
              "body": {
                "value": "@json(concat('{ \"', item().paramName, '\": \"', item().paramValue, '\" }'))",
                "type": "Expression"
              },
              "method": "POST"
            },
            "linkedServiceName": {
              "referenceName": azurerm_data_factory_linked_service_azure_function.azure_func.name,
              "type": "LinkedServiceReference"
            }
          }
        ]
      }
    }
  ])
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
        activity          = "bronze"
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
        activity          = "silver"
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