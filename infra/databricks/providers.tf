# infra/databricks/providers.tf (PO KOREKCIE)

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0" # Match your desired version
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.28" # Match your desired version
    }
  }
}

# Provider Databricks na poziomie workspace (do innych zasobów)
# provider "databricks" {
#   host = azurerm_databricks_workspace.this.workspace_url
# }

# # Provider Databricks na poziomie account (do metastore assignment)
# provider "databricks" {
#   alias      = "account"
#   host       = "https://accounts.cloud.databricks.com"
#   account_id = "66c1b241-83fc-4843-a4ea-ce7af124c8f9"
#   token      = "dsapicf1805995a6693da8b21cc9e8253ed3a"
# }