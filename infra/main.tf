# --- Zmienne lokalne (locals) ---
locals {
  # Unikalny sufiks dla nazw zasobów, bazujący na ID grupy zasobów
  # Użycie funkcji sha1() na ID resource group zapewni unikalność po pierwszym utworzeniu RG
  unique_suffix = substr(sha1(azurerm_resource_group.rg_functions.id), 0, 4)

  # Finalne nazwy zasobów (z możliwością nadpisania przez zmienne wejściowe)
  function_app_name_final           = var.function_app_name != "" ? var.function_app_name : "${var.project_prefix}${var.environment}func${local.unique_suffix}"
  function_storage_account_name_final = lower(replace("${var.project_prefix}${var.environment}func${local.unique_suffix}sa", "-", "")) # Nazwy kont storage muszą być małe litery i bez myślników

  # Ścieżki do kodu funkcji i skryptu do pakowania
  # source_code_dir to katalog główny projektu 'CV-DEMO1'
  source_code_dir        = abspath("${path.module}/..")
  
  # output_zip_file to plik ZIP, który zostanie utworzony w katalogu 'CV-DEMO1'
  #output_zip_file        = abspath("${path.module}/../function_app_package.zip")
  # create_zip_script_path to ścieżka do skryptu Pythona do pakowania
  #create_zip_script_path = abspath("${path.module}/../tools/create_zip.py")
}

## --- Zasoby Infrastruktury Azure ---

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
    FUNCTIONS_WORKER_RUNTIME            = "python"                                     # Określa runtime funkcji (python, dotnet, node, java, powershell)
    FUNCTIONS_WORKER_RUNTIME_VERSION    = "~3.12"                                      # Wersja języka Python
    APPLICATIONINSIGHTS_CONNECTION_STRING = azurerm_application_insights.app_insights.connection_string
    AzureWebJobsStorage                 = azurerm_storage_account.sa_functions.primary_connection_string
    FUNCTIONS_EXTENSION_VERSION         = "~4"                                       # Wersja rozszerzenia funkcji (dla blueprintów)
    "AzureWebJobsFeatureFlags"          = "EnableWorkerIndexing"                     # Włącz indexowanie workerów dla Python V2 (blueprinty)
    # WEBSITE_RUN_FROM_PACKAGE nie jest tutaj ustawiany, zostanie ustawiony automatycznie przez az functionapp deployment source config-zip
  }

  tags = {
    Environment = var.environment
    Project     = var.project_prefix
  }
}

module "adf" {
  source                  = "./adf"
  # WAŻNE: Nazwa przekazywana do modułu ADF powinna być teraz 'adf_name'
  # i powinna być generowana z Twoich prefixów.
  adf_name                = "${var.project_prefix}-${var.environment}-df"
  resource_group_name     = azurerm_resource_group.rg_functions.name
  location                = var.location
  project_prefix          = var.project_prefix
  function_app_url        = azurerm_function_app.main_function_app.default_hostname
}