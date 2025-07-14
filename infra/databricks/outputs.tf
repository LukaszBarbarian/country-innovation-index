# infra/databricks/outputs.tf (NOWY PLIK)


output "databricks_workspace_id" {
  description = "The ID of the Databricks Workspace created by this module."
  value       = azurerm_databricks_workspace.this.id
}

output "databricks_workspace_url" {
  description = "The URL of the Databricks Workspace created by this module."
  value       = azurerm_databricks_workspace.this.workspace_url
}


output "databricks_access_connector_id" {
  description = "The ID of the Databricks Access Connector."
  value       = azurerm_databricks_access_connector.uc_adls_connector.id
}

# output "databricks_secret_scope_name" {
#   description = "The name of the Databricks secret scope for Key Vault."
#   value       = databricks_secret_scope.keyvault_scope.name
# }