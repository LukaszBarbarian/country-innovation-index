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