variable "resource_group_name" {
  description = "The name of the resource group."
  type        = string
}

variable "location" {
  description = "Azure region."
  type        = string
}

variable "data_factory_name" {
  description = "Name for the Data Factory instance."
  type        = string
}


variable "function_app_url" {
  description = "URL do funkcji Azure Function App, używany w Linked Service"
  type        = string
}
