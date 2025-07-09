# terraform/variables.tf

# Globalne zmienne projektu
variable "project_prefix" {
  description = "A unique prefix for all resources in this project (e.g., your initials + cv)."
  type        = string
  default     = "demosur"
}

variable "resource_group_name" {
  description = "The name of the resource group for shared modules like ADF"
  type        = string
  default     = "" # zostanie nadpisane w `main.tf`, więc może być pusty
}

variable "location" {
  description = "The Azure region where resources will be deployed."
  type        = string
  default     = "northeurope"
}

variable "environment" {
  description = "The environment name (e.g., dev, test, prod)."
  type        = string
  default     = "dev"
}

variable "app_service_plan_sku" {
  description = "The SKU for the App Service Plan (e.g., Y1, EP1, S1)."
  type        = string
  default     = "Y1"
}

variable "app_service_plan_os_type" {
  description = "The OS type for the App Service Plan (Linux or Windows)."
  type        = string
  default     = "linux"
}

variable "function_app_name" {
  description = "The globally unique name for the Azure Function App. If not provided, a unique name will be generated."
  type        = string
  default     = "" 
}

variable "function_storage_account_sku" {
  description = "The SKU for the Storage Account for the Function App."
  type        = string
  default     = "LRS"
}

variable "function_storage_account_kind" {
  description = "The Kind for the Storage Account for the Function App."
  type        = string
  default     = "StorageV2"
}

variable "app_insights_type" {
  description = "The Application Insights kind (e.g., web, general)."
  type        = string
  default     = "web"
}

variable "app_insights_daily_cap" {
  description = "The daily data volume cap in GB."
  type        = number
  default     = 100
}


variable "function_code_zip_output_relative_path" {
  description = "Relative path for the output function app ZIP file"
  type        = string
  default     = "function_app_package.zip"
}
