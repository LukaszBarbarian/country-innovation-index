



locals {
  # Unikalny sufiks jest już zdefiniowany w main.tf, użyjemy go tutaj
  eventgrid_topic_name_final = "${var.project_prefix}-${var.environment}-etl-events-topic"
}

# --- Zasoby Event Grid ---

### Utworzenie niestandardowego tematu Event Grid
resource "azurerm_eventgrid_topic" "etl_events_topic" {
  name                = local.eventgrid_topic_name_final
  location            = azurerm_resource_group.rg_functions.location
  resource_group_name = azurerm_resource_group.rg_functions.name

  tags = {
    Environment = var.environment
    Project     = var.project_prefix
  }
}

### Nadanie uprawnień Function App do wysyłania zdarzeń do tematu Event Grid
# Używamy tutaj tożsamości zarządzanej Function App
resource "azurerm_role_assignment" "function_app_event_grid_publisher" {
  scope                = azurerm_eventgrid_topic.etl_events_topic.id
  role_definition_name = "EventGrid Data Sender"
  principal_id         = azurerm_function_app.main_function_app.identity[0].principal_id
}

