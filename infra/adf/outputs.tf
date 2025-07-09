# infra/adf/outputs.tf

output "data_factory_id" {
  description = "The ID of the created Azure Data Factory instance."
  value       = azurerm_data_factory.adf_instance.id # <--- Zmieniono z azurerm_data_factory.main.id na azurerm_data_factory.adf_instance.id
}