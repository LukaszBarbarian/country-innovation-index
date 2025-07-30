resource "azurerm_storage_account" "sadatalake" { # Zmieniona nazwa zmiennej lokalnej
  name                     = "${replace(var.project_prefix, "-", "")}${var.environment}datalake${local.unique_suffix}sa" # Nazwa będzie teraz np. 'demosur1devdatalake51d9sa'
  resource_group_name      = azurerm_resource_group.rg_functions.name
  location                 = azurerm_resource_group.rg_functions.location
  account_tier             = "Standard"
  account_replication_type = "GRS" # Geo-Redundant Storage (lub LRS/ZRS w zależności od potrzeb)
  is_hns_enabled           = true # Włączanie Hierarchical Namespace (Data Lake Gen2)

  tags = {
    Environment = var.environment
    Purpose     = "DataLake"
  }
}

resource "azurerm_role_assignment" "function_app_data_lake_contributor" {
  # Scope: Konto Data Lake Storage
  scope                = azurerm_storage_account.sadatalake.id
  # Rola, która pozwala na zapisywanie i modyfikowanie danych w blobach
  role_definition_name = "Storage Blob Data Contributor"
  # Principal ID tożsamości zarządzanej Twojej Function App
  principal_id         = azurerm_function_app.main_function_app.identity[0].principal_id
}

resource "azurerm_storage_container" "container_configs" {
  name                  = "configs"
  storage_account_name  = azurerm_storage_account.sadatalake.name
  container_access_type = "private" # Domyślnie prywatny
}

# Kontener (system plików/folder) dla warstwy Bronze
resource "azurerm_storage_container" "container_bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.sadatalake.name
  container_access_type = "private" # Domyślnie prywatny
}

# Kontener (system plików/folder) dla warstwy Silver
resource "azurerm_storage_container" "container_silver" {
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.sadatalake.name
  container_access_type = "private"
}

# Kontener (system plików/folder) dla warstwy Gold
resource "azurerm_storage_container" "container_gold" {
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.sadatalake.name
  container_access_type = "private"
}