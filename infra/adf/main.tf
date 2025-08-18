# adf/main.tf

# ... Your existing code for the Data Factory instance ...
resource "azurerm_data_factory" "adf_instance" {
  name                = var.adf_name 
  location            = var.location
  resource_group_name = var.resource_group_name

  tags = {
    Environment = var.environment
    Project     = var.project_prefix
  }

  identity {
    type = "SystemAssigned"
  }


  # Dodaj poniższy blok, aby zdefiniować globalne zmienne
  global_parameter {
    name  = "bronze_manifest_url"
    type  = "String" # Typ danych
    value = format("https://%s.dfs.core.windows.net/%s/manifest", var.storage_account_name, var.bronze_container_name)
  }

  global_parameter {
    name  = "silver_manifest_url"
    type  = "String"
    value = format("https://%s.dfs.core.windows.net/%s/manifest", var.storage_account_name, var.silver_container_name)
  }

  global_parameter {
    name  = "gold_manifest_url"
    type  = "String"
    value = format("https://%s.dfs.core.windows.net/%s/manifest", var.storage_account_name, var.gold_container_name)
  }

  global_parameter {
    name  = "bronze_ingestion_summary_url"
    type  = "String"
    value = format("https://%s.dfs.core.windows.net/%s/outputs/ingestion_summaries", var.storage_account_name, var.bronze_container_name)
  }
}

resource "azurerm_data_factory_trigger_custom_event" "bronze_trigger" {
  name                = "bronze-ingestion-trigger"
  data_factory_id     = azurerm_data_factory.adf_instance.id
  eventgrid_topic_id  = var.eventgrid_topic_id

  events = ["StartBronzeIngestion"]

  pipeline {
    name = "bronze_pipeline"
    parameters = {
      p_payload = "@triggerBody().event"
    }
  }
}

resource "azurerm_data_factory_trigger_custom_event" "silver_trigger" {
  name                = "silver-ingestion-trigger"
  data_factory_id     = azurerm_data_factory.adf_instance.id
  eventgrid_topic_id  = var.eventgrid_topic_id

  events = ["BronzeIngestionCompleted"]

  pipeline {
    name = "silver_pipeline"
    parameters = {
      p_payload = "@triggerBody().event"
    }
  }
}

resource "azurerm_data_factory_trigger_schedule" "bronze_daily_trigger" {
  name            = "bronze-daily-trigger"
  data_factory_id = azurerm_data_factory.adf_instance.id

  pipeline {
    name = "bronze_pipeline"
  }

  frequency = "Day"
  interval  = 1

  schedule {
    minutes = [0]
    hours   = [0]
  }
}