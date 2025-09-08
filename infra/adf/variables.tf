# infra/adf/variables.tf

variable "resource_group_name" {
  description = "The name of the resource group where ADF will be deployed."
  type        = string
}

variable "location" {
  description = "The Azure region where ADF will be deployed."
  type        = string
}

variable "adf_name" {
  description = "The name of the Azure Data Factory instance."
  type        = string
}

variable "project_prefix" {
  description = "The project prefix used for resource naming."
  type        = string
}

variable "function_app_url" {
  description = "The default hostname (URL) of the Azure Function App to link with ADF."
  type        = string
}



variable "environment" {
  description = "The environment name (e.g., dev, test, prod)."
  type        = string
  default     = "dev"
}


variable "eventgrid_topic_id" {
  description = "The ID of the Event Grid Topic to which the triggers will subscribe."
  type        = string
}


variable "storage_account_primary_connection_string" {
  type        = string
  description = "Primary connection string do Storage Account (Bronze/Silver/Gold)"
}



variable "bronze_container_name" {
  type = string
}

variable "silver_container_name" {
  type = string
}

variable "gold_container_name" {
  type = string
}

variable "storage_account_name" {
  type = string
}


variable "app_config_endpoint" {
  type = string
}