# terraform/providers.tf


terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0" # Upewnij się, że używasz odpowiedniej wersji
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.28" # Upewnij się, że używasz odpowiedniej wersji
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id = "4f24dcd1-9601-4727-ab67-d97cebce64ca"
}


provider "databricks" {
  # Autentykacja na poziomie konta (Account-level authentication)
  # Wymaga ustawienia zmiennych środowiskowych:
  # DATABRICKS_ACCOUNT_ID (Twój Databricks Account ID)
  # DATABRICKS_CLIENT_ID (Service Principal Application (client) ID z Account Console)
  # DATABRICKS_CLIENT_SECRET (Service Principal Secret z Account Console)
  # LUB DATABRICKS_USERNAME i DATABRICKS_PASSWORD (dla użytkownika konta)

  # Nie podawaj tutaj bezpośrednio wrażliwych danych!
  # Host NIE jest potrzebny dla operacji na poziomie konta
}
