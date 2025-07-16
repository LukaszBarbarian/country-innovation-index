# --- Data Source do pobrania ID tenanta i Object ID bieżącego użytkownika/SP ---
data "azurerm_client_config" "current" {}


# --- Azure Key Vault ---
resource "azurerm_key_vault" "main_keyvault" {
  name                = "${var.project_prefix}-${var.environment}-kv"
  location            = azurerm_resource_group.rg_functions.location
  resource_group_name = azurerm_resource_group.rg_functions.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  tags = {
    Environment = var.environment
    Project     = var.project_prefix
    Purpose     = "Secrets"
  }
}
  resource "azurerm_key_vault_access_policy" "terraform_access" {
  key_vault_id = azurerm_key_vault.main_keyvault.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = data.azurerm_client_config.current.object_id

  secret_permissions = [
    "Get",
    "List",
    "Set",
    "Delete",
    "Purge",
    "Recover",
  ]
}

# ... (Pozostałe sekrety) ...
resource "azurerm_key_vault_secret" "databricks_access_connector_id_secret" {
  name         = "databricks-access-connector-id"
  value        = module.databricks.databricks_access_connector_id
  key_vault_id = azurerm_key_vault.main_keyvault.id
  content_type = "text/plain"
  depends_on   = [
    # Zmieniamy zależność, aby wskazywała na główny zasób Key Vault
    azurerm_key_vault.main_keyvault,
    module.databricks.databricks_access_connector_id
  ]
}

resource "azurerm_key_vault_secret" "datalake_storage_account_name_secret" {
  name         = "datalake-storage-account-name"
  value        = azurerm_storage_account.sadatalake.name
  key_vault_id = azurerm_key_vault.main_keyvault.id
  content_type = "text/plain"
  depends_on   = [
    azurerm_key_vault.main_keyvault
  ]
}