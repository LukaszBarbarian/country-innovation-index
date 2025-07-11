# infra/databricks/outputs.tf (NOWY PLIK)


output "databricks_workspace_id" {
  description = "The ID of the Databricks Workspace created by this module."
  value       = azurerm_databricks_workspace.this.id
}

output "databricks_workspace_url" {
  description = "The URL of the Databricks Workspace created by this module."
  value       = azurerm_databricks_workspace.this.workspace_url
}