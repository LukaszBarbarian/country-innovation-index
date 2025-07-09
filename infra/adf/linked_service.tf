# infra/adf/linked_service.tf

resource "azurerm_data_factory_linked_service_azure_function" "azure_func" {
  name                = "azure_function_linked_service" # Nazwa zgodna z Twoim JSON
  data_factory_id     = azurerm_data_factory.adf_instance.id
    url                 = var.function_app_url # URL Function App będzie dynamicznie wstrzykiwany
    key = "anonymous_function_key" # lub cokolwiek innego, np. "dummy"
}