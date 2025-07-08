# terraform/outputs.tf

output "resource_group_name" {
  description = "The name of the created Resource Group for functions."
  value       = azurerm_resource_group.rg_functions.name
}

output "function_app_name" {
  description = "The name of the deployed Azure Function App."
  value       = azurerm_function_app.main_function_app.name
}


output "function_app_default_hostname" {
  description = "The default hostname of the Function App."
  value       = azurerm_function_app.main_function_app.default_hostname
}

output "storage_account_name" {
  description = "The name of the Storage Account used by the Function App."
  value       = azurerm_storage_account.sa_functions.name
}

output "application_insights_instrumentation_key" {
  description = "The instrumentation key for Application Insights."
  value       = azurerm_application_insights.app_insights.instrumentation_key
  sensitive   = true
}

output "application_insights_connection_string" {
  description = "The connection string for Application Insights."
  value       = azurerm_application_insights.app_insights.connection_string
  sensitive   = true
}


output "data_lake_storage_account_name" {
  description = "The name of the main Data Lake Storage Account for Bronze/Silver/Gold layers."
  value       = azurerm_storage_account.sadatalake.name
}

output "bronze_container_name" {
  description = "The name of the Bronze layer container."
  value       = azurerm_storage_container.container_bronze.name
}
output "silver_container_name" {
  description = "The name of the Silver layer container."
  value       = azurerm_storage_container.container_silver.name
}
output "gold_container_name" {
  description = "The name of the Gold layer container."
  value       = azurerm_storage_container.container_gold.name
}