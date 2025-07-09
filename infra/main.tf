# --- Zmienne lokalne (locals) ---
locals {
  unique_suffix = substr(sha1(azurerm_resource_group.rg_functions.id), 0, 4)

  function_app_name_final           = var.function_app_name != "" ? var.function_app_name : "${var.project_prefix}${var.environment}func${local.unique_suffix}"
  function_storage_account_name_final = lower(replace("${var.project_prefix}${var.environment}func${local.unique_suffix}sa", "-", "")) # Nazwy kont storage muszą być małe litery i bez myślników

  data_factory_name_final = "${var.project_prefix}-${var.environment}-adf-${local.unique_suffix}"
  source_code_dir        = abspath("${path.module}/..")
}


### 1. Resource Group
resource "azurerm_resource_group" "rg_functions" {
  name     = "${var.project_prefix}-${var.environment}-rg"
  location = var.location

  tags = {
    Environment = var.environment
    Project     = var.project_prefix
  }
}

### 2. Storage Account dla Function App
# Wymagany przez Azure Functions do przechowywania danych runtime i logów
resource "azurerm_storage_account" "sa_functions" {
  name                     = local.function_storage_account_name_final
  resource_group_name      = azurerm_resource_group.rg_functions.name
  location                 = azurerm_resource_group.rg_functions.location
  account_tier             = "Standard"
  account_replication_type = var.function_storage_account_sku
  account_kind             = var.function_storage_account_kind
  min_tls_version          = "TLS1_2"

  tags = {
    Environment = var.environment
    Project     = var.project_prefix
  }
}

### 3. App Service Plan
# Określa środowisko hostingu (np. zużycie, dedykowane) i system operacyjny
resource "azurerm_service_plan" "app_service_plan" {
  name                = "${var.project_prefix}-${var.environment}-asp"
  location            = azurerm_resource_group.rg_functions.location
  resource_group_name = azurerm_resource_group.rg_functions.name
  os_type             = title(var.app_service_plan_os_type) # Np. "Linux" lub "Windows"
  sku_name            = var.app_service_plan_sku             # Np. "Y1" (Consumption), "B1" (Basic), "S1" (Standard)

  tags = {
    Environment = var.environment
    Project     = var.project_prefix
  }
}

### 4. Application Insights
# Do monitorowania i logowania funkcji
resource "azurerm_application_insights" "app_insights" {
  name                = "${var.project_prefix}-${var.environment}-appi"
  location            = azurerm_resource_group.rg_functions.location
  resource_group_name = azurerm_resource_group.rg_functions.name
  application_type    = var.app_insights_type

  tags = {
    Environment = var.environment
    Project     = var.project_prefix
  }
}

### 5. Function App
# Główny zasób dla Twojej funkcji
resource "azurerm_function_app" "main_function_app" {
  name                        = local.function_app_name_final
  location                    = azurerm_resource_group.rg_functions.location
  resource_group_name         = azurerm_resource_group.rg_functions.name
  app_service_plan_id         = azurerm_service_plan.app_service_plan.id
  storage_account_name        = azurerm_storage_account.sa_functions.name
  storage_account_access_key  = azurerm_storage_account.sa_functions.primary_access_key

  version    = "~4"
  #os_type    = var.app_service_plan_os_type
  https_only = true

  # Ustawienia aplikacji dla Function App
  app_settings = {
    FUNCTIONS_WORKER_RUNTIME            = "python"                                    
    FUNCTIONS_WORKER_RUNTIME_VERSION    = "~3.12"                                     
    APPLICATIONINSIGHTS_CONNECTION_STRING = azurerm_application_insights.app_insights.connection_string
    AzureWebJobsStorage                 = azurerm_storage_account.sa_functions.primary_connection_string
    FUNCTIONS_EXTENSION_VERSION         = "~4"                                      
    AzureFunctionsWebJobsFeatureFlags = "EnableWorkerExtension" 
    WEBSITE_RUN_FROM_PACKAGE          = "1"
  }

  tags = {
    Environment = var.environment
    Project     = var.project_prefix
  }
}


module "adf_factory" {
  source               = "./adf"
  resource_group_name  = azurerm_resource_group.rg_functions.name
  location             = azurerm_resource_group.rg_functions.location
  data_factory_name    = local.data_factory_name_final
  function_app_url     = "https://${azurerm_function_app.main_function_app.default_hostname}/api"

}