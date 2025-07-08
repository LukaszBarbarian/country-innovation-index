# terraform/backend.tf
terraform {
  backend "azurerm" {
    resource_group_name  = "tfstate-backend-rg-lukasz" # Nazwa nowej grupy zasobów dla backendu
    storage_account_name = "tfstatelukaszcv123"       # Nazwa NOWEGO konta magazynowego dla backendu (jak stworzyłeś w kroku 3)
    container_name       = "tfstate"
    key                  = "terraform.tfstate"
  }
}