resource "azurerm_storage_account" "sa_queue" {
  name                     = local.queue_storage_account_name_final
  resource_group_name      = azurerm_resource_group.rg_functions.name
  location                 = azurerm_resource_group.rg_functions.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  min_tls_version          = "TLS1_2"

  tags = {
    Environment = var.environment
    Project     = var.project_prefix
  }
}

resource "azurerm_storage_queue" "queue_bronze_tasks" {
  name                 = local.queue_name_final
  storage_account_name = azurerm_storage_account.sa_queue.name
}

# Nowy zasób, który tworzy kolejkę dla procesu silver
resource "azurerm_storage_queue" "queue_silver_processing" {
  name                 = "silver-processing-queue"
  storage_account_name = azurerm_storage_account.sa_queue.name
}

# Ewentualnie role dla funkcji (jeśli potrzebujesz)
resource "azurerm_role_assignment" "function_app_queue_contributor" {
  scope                = azurerm_storage_account.sa_queue.id
  role_definition_name = "Storage Queue Data Contributor"
  principal_id         = azurerm_function_app.main_function_app.identity[0].principal_id
}