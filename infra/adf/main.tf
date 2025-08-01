# infra/adf/main.tf (lub inne pliki w module adf)

resource "azurerm_data_factory" "adf_instance" {
  name                = var.adf_name 
  location            = var.location
  resource_group_name = var.resource_group_name

  tags = {
    Environment = var.environment
    Project     = var.project_prefix
  }

  identity {
    type = "SystemAssigned"
  }
}

