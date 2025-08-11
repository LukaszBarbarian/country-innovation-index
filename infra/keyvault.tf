
# --- Azure Key Vault ---
resource "azurerm_key_vault" "main_keyvault" {
  name                = "${var.project_prefix}-${var.environment}-kv"
  location            = azurerm_resource_group.rg_functions.location
  resource_group_name = azurerm_resource_group.rg_functions.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  #soft_delete_enabled = true

  access_policy {
  tenant_id = data.azurerm_client_config.current.tenant_id
  object_id = azurerm_function_app.main_function_app.identity[0].principal_id

  secret_permissions = [
  "Get",
  "List",
  "Set",
  "Delete",
  "Purge",
  "Recover"
]
}

  tags = {
    Environment = var.environment
    Project     = var.project_prefix
    Purpose     = "Secrets"
  }
}


resource "azurerm_key_vault_access_policy" "terraform" {
  key_vault_id = azurerm_key_vault.main_keyvault.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = data.azurerm_client_config.current.object_id

  secret_permissions = [
  "Get",
  "List",
  "Set",
  "Delete",
  "Purge",
  "Recover"
]
}



resource "azurerm_key_vault_access_policy" "sp_policy" {
  key_vault_id = azurerm_key_vault.main_keyvault.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = "ce3ce6ed-f684-4d72-8467-44c6bd01add3"

  secret_permissions = [
    "Get",
    "List"
  ]
}


resource "azurerm_key_vault_secret" "storage_account_name" {
  name         = "storage-account-name"
  value        = azurerm_storage_account.sadatalake.name
  key_vault_id = azurerm_key_vault.main_keyvault.id

  depends_on = [
    azurerm_storage_account.sadatalake,
    azurerm_key_vault.main_keyvault,
    azurerm_key_vault_access_policy.terraform
  ]
}

resource "azurerm_key_vault_secret" "storage_container_configs_name" {
  name         = "storage-container-configs-name"
  value        = azurerm_storage_container.container_configs.name
  key_vault_id = azurerm_key_vault.main_keyvault.id

  depends_on = [
    azurerm_storage_account.sa_functions,
    azurerm_key_vault.main_keyvault,
    azurerm_key_vault_access_policy.terraform
  ]
}

resource "azurerm_key_vault_secret" "eventgrid_topic_name" {
  name         = "eventgrid-topic-name"
  value        = azurerm_eventgrid_topic.etl_events_topic.name
  key_vault_id = azurerm_key_vault.main_keyvault.id


depends_on = [
    azurerm_storage_account.sa_functions,
    azurerm_key_vault.main_keyvault,
    azurerm_key_vault_access_policy.terraform
  ]
}

resource "azurerm_key_vault_secret" "resource_group_name" {
  name         = "resource-group-name"
  value        = azurerm_resource_group.rg_functions.name
  key_vault_id = azurerm_key_vault.main_keyvault.id
  
  depends_on = [
    azurerm_storage_account.sa_functions,
    azurerm_key_vault.main_keyvault,
    azurerm_key_vault_access_policy.terraform
  ]
}

