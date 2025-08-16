# infra/adf/linked_service.tf

resource "azurerm_data_factory_linked_service_azure_function" "azure_func" {
  name            = "azure_function_linked_service"
  data_factory_id = azurerm_data_factory.adf_instance.id
  url             = "https://${var.function_app_url}"
  key             = "anonymous_function_key" # Zmień to, jeśli Twoja funkcja wymaga klucza
}



resource "azurerm_data_factory_linked_service_azure_blob_storage" "bronze_storage" {
  name            = "bronze_storage_linked_service"
  data_factory_id = azurerm_data_factory.adf_instance.id
  connection_string = var.storage_account_primary_connection_string
}