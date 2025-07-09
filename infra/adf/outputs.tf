output "data_factory_name" {
  description = "The name of the deployed Azure Data Factory."
  # TUTAJ ZMIANA: Zmieniamy 'adf_main' na 'adf_instance'
  value       = azurerm_data_factory.adf_instance.name
}

output "data_factory_id" {
  description = "The ID of the deployed Azure Data Factory."
  # Opcjonalnie, jeśli chcesz eksportować ID fabryki danych
  value       = azurerm_data_factory.adf_instance.id
}