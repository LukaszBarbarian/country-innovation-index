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
  
  # WAŻNE: Usuń blok 'access_policy' Z TEGO MIEJSCA, jeśli był!
  # Nie powinien już być w tym zasobie, jeśli używasz oddzielnego azurerm_key_vault_access_policy.
}



# --- Polityka dostępu dla Terraform (aby mógł zapisywać sekrety) ---
resource "azurerm_key_vault_access_policy" "terraform_access" {
  key_vault_id = azurerm_key_vault.main_keyvault.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  # Użyj object_id bieżącego użytkownika/Service Principal, który uruchamia Terraform
  object_id    = data.azurerm_client_config.current.object_id # Bardziej dynamiczne niż hardkodowanie

  secret_permissions = [
    "Get",
    "List",
    "Set",
  ]
  
  lifecycle {
    prevent_destroy = false
  }
}


resource "azurerm_key_vault_secret" "databricks_access_connector_id_secret" {
  name         = "databricks-access-connector-id"
  # TUTAJ JEST ZMIANA: Odwołujemy się do outputu modułu databricks
  value        = module.databricks.databricks_access_connector_id
  key_vault_id = azurerm_key_vault.main_keyvault.id
  content_type = "text/plain"
  depends_on   = [
    azurerm_key_vault_access_policy.terraform_access,
    # Ważne: Zmieniamy zależność z zasobu na output modułu
    module.databricks.databricks_access_connector_id
  ]
}

# ... (pozostałe sekrety, np. dla nazwy konta storage, które już są poprawne) ...
resource "azurerm_key_vault_secret" "datalake_storage_account_name_secret" {
  name         = "datalake-storage-account-name"
  value        = azurerm_storage_account.sadatalake.name
  key_vault_id = azurerm_key_vault.main_keyvault.id
  content_type = "text/plain"
  depends_on   = [azurerm_key_vault_access_policy.terraform_access]
}