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
}

resource "azurerm_data_factory_trigger_custom_event" "bronze_trigger" {
  name                = "bronze-ingestion-trigger"
  data_factory_id     = azurerm_data_factory.adf_instance.id
  eventgrid_topic_id  = var.eventgrid_topic_id

  events = ["StartBronzeIngestion"]

  pipeline {
    name = "bronze_pipeline"
    parameters = {
      p_payload = "@triggerBody()"
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
      p_payload = "@triggerBody()"
    }
  }
}