# infra/databricks/main.tf (PO KOREKCIE)

resource "azurerm_databricks_workspace" "this" {
  name                = var.databricks_name
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "premium"

  tags = {
    Environment = var.environment
    Project     = var.project_prefix
  }
}


resource "databricks_metastore_assignment" "this" {
  provider = databricks.account

  metastore_id         = "4f24dcd1-9601-4727-ab67-d97cebce64ca"
  workspace_id         = "3391578650895245"
  default_catalog_name = "main"
}


# resource "azurerm_user_assigned_identity" "databricks_adls_mi" {
#   name                = "${var.project_prefix}-${var.environment}-databricks-adls-mi"
#   resource_group_name = var.resource_group_name
#   location            = var.location
# }

# # Przypisanie roli dla User-Assigned MI do Storage Account
# resource "azurerm_role_assignment" "databricks_adls_mi_access" {
#   scope                = var.storage_account_id # <--- UŻYJ ZMIENNEJ WEJŚCIOWEJ
#   role_definition_name = "Storage Blob Data Contributor"
#   principal_id         = azurerm_user_assigned_identity.databricks_adls_mi.principal_id
# }

# # Databricks Access Connector dla Unity Catalog
# resource "azurerm_databricks_access_connector" "uc_adls_connector" {
#   name                = "${var.project_prefix}-${var.environment}-uc-adls-connector"
#   resource_group_name = var.resource_group_name
#   location            = var.location

#   identity {
#     type = "SystemAssigned"
#   }
# }

# # Przypisanie roli dla Access Connector do Storage Account
# resource "azurerm_role_assignment" "uc_adls_connector_access" {
#   scope                = var.storage_account_id # <--- UŻYJ ZMIENNEJ WEJŚCIOWEJ
#   role_definition_name = "Storage Blob Data Contributor"
#   principal_id         = azurerm_databricks_access_connector.uc_adls_connector.identity[0].principal_id
# }

# # Databricks Storage Credential
# resource "databricks_storage_credential" "adls_credential" {
#   name           = "${var.project_prefix}_${var.environment}_adls_credential"
#   comment        = "Credential for ADLS Gen2 access via Unity Catalog"
#   azure_managed_identity {
#     access_connector_id = azurerm_databricks_access_connector.uc_adls_connector.id
#   }
# }

# # Databricks External Location
# resource "databricks_external_location" "adls_external_location_bronze" {
#   name          = "${var.project_prefix}_${var.environment}_adls_location_bronze"
#   url           = "abfss://${var.bronze_container_name}@${var.storage_account_name}.dfs.core.windows.net/" # Użycie zmiennych wejściowych
#   credential_name = databricks_storage_credential.adls_credential.name # <--- ZMIENIONE Z credential_id NA credential_name
# }


# # Databricks External Location
# resource "databricks_external_location" "adls_external_location_silver" {
#   name          = "${var.project_prefix}_${var.environment}_adls_location_silver"
#   url           = "abfss://${var.silver_container_name}@${var.storage_account_name}.dfs.core.windows.net/" # Użycie zmiennych wejściowych
#   credential_name = databricks_storage_credential.adls_credential.name # <--- ZMIENIONE Z credential_id NA credential_name
# }



# resource "databricks_notebook" "bronze_ingest_notebook" {
#   path     = "/bronze/ingest_raw_data"
#   language = "PYTHON"
#   source = "${path.module}/../../databricks/test.py"
#   format   = "SOURCE"
# }


# resource "databricks_dbfs_file" "shared_init" {
#   source = "${path.module}/../../shared/__init__.py"
#   path   = "/FileStore/libs/shared/__init__.py"
# }

# resource "databricks_dbfs_file" "shared_enums" {
#   source = "${path.module}/../../shared/enums/api_type.py"
#   path   = "/FileStore/libs/shared/enums.py"
# }

# resource "databricks_dbfs_file" "shared_factories" {
#   source = "${path.module}/../../shared/factories/base.py"
#   path   = "/FileStore/libs/shared/factories.py"
# }
