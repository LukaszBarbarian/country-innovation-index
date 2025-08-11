# infra/adf/outputs.tf

output "data_factory_id" {
  description = "The ID of the created Azure Data Factory instance."
  value       = azurerm_data_factory.adf_instance.id # <--- Zmieniono z azurerm_data_factory.main.id na azurerm_data_factory.adf_instance.id
}


output "adf_principal_id" {
  description = "The Principal ID of the Azure Data Factory's Managed Service Identity."
  value       = azurerm_data_factory.adf_instance.identity[0].principal_id
}

