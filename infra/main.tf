data "azurerm_client_config" "current" {}


locals {
  # Unikalny sufiks dla nazw zasobów, bazujący na ID grupy zasobów
  # Użycie funkcji sha1() na ID resource group zapewni unikalność po pierwszym utworzeniu RG
  unique_suffix = substr(sha1(azurerm_resource_group.rg_functions.id), 0, 4)

  # Finalne nazwy zasobów (z możliwością nadpisania przez zmienne wejściowe)
  function_app_name_final           = var.function_app_name != "" ? var.function_app_name : "${var.project_prefix}${var.environment}func${local.unique_suffix}"
  function_storage_account_name_final = lower(replace("${var.project_prefix}${var.environment}func${local.unique_suffix}sa", "-", "")) # Nazwy kont storage muszą być małe litery i bez myślników
  queue_storage_account_name_final = lower(replace("${var.project_prefix}${var.environment}queue${local.unique_suffix}sa", "-", "")) # Nowe konto storage dla kolejki
  queue_name_final = "bronze-tasks"


  # Ścieżki do kodu funkcji i skryptu do pakowania
  # source_code_dir to katalog główny projektu 'CV-DEMO1'
  source_code_dir        = abspath("${path.module}/..")

  # output_zip_file to plik ZIP, który zostanie utworzony w katalogu 'CV-DEMO1'
  #output_zip_file         = abspath("${path.module}/../function_app_package.zip")
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


resource "azurerm_role_assignment" "function_app_blob_data_contributor" {
  scope                = azurerm_storage_account.sa_functions.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_function_app.main_function_app.identity[0].principal_id
}


### 3. App Service Plan
# Określa środowisko hostingu (np. zużycie, dedykowane) i system operacyjny
resource "azurerm_service_plan" "app_service_plan" {
  name                = "${var.project_prefix}-${var.environment}-asp"
  location            = azurerm_resource_group.rg_functions.location
  resource_group_name = azurerm_resource_group.rg_functions.name
  os_type             = title(var.app_service_plan_os_type) # Np. "Linux" lub "Windows"
  sku_name            = var.app_service_plan_sku            # Np. "Y1" (Consumption), "B1" (Basic), "S1" (Standard)

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

  # Dodaj tożsamość zarządzaną, jeśli jeszcze jej nie masz.
  # Jest ona potrzebna do nadawania uprawnień (np. do kolejki)
  identity {
    type = "SystemAssigned"
  }

  version    = "~4"
  https_only = true

  # Ustawienia aplikacji dla Function App
  app_settings = {
    FUNCTIONS_WORKER_RUNTIME          = "python"                                          # Określa runtime funkcji (python, dotnet, node, java, powershell)
    FUNCTIONS_WORKER_RUNTIME_VERSION  = "~3.12"                                           # Wersja języka Python
    APPLICATIONINSIGHTS_CONNECTION_STRING = azurerm_application_insights.app_insights.connection_string
    AzureWebJobsStorage               = azurerm_storage_account.sa_functions.primary_connection_string
    FUNCTIONS_EXTENSION_VERSION       = "~4"                                              # Wersja rozszerzenia funkcji (dla blueprintów)
    "AzureWebJobsFeatureFlags"        = "EnableWorkerIndexing"                            # Włącz indexowanie workerów dla Python V2 (blueprinty)
    DATA_LAKE_STORAGE_ACCOUNT_NAME = azurerm_storage_account.sadatalake.name
    QUEUE_NAME = azurerm_storage_queue.queue_bronze_tasks.name
    QUEUE_STORAGE_ACCOUNT = azurerm_storage_account.sa_queue.name
    QUEUE_CONNECTION_STRING = azurerm_storage_account.sa_queue.primary_connection_string
    NOBELPRIZE_API_BASE_URL = "https://api.nobelprize.org/2.1/"
    AzureWebJobsStorageQueue = azurerm_storage_account.sa_queue.primary_connection_string
    EVENT_GRID_ENDPOINT             = azurerm_eventgrid_topic.etl_events_topic.endpoint
    EVENT_GRID_KEY                  = azurerm_eventgrid_topic.etl_events_topic.primary_access_key



  }

  tags = {
    Environment = var.environment
    Project     = var.project_prefix
  }
}

module "adf" {
  source                    = "./adf"
  adf_name                  = "${var.project_prefix}-${var.environment}-df"
  resource_group_name       = azurerm_resource_group.rg_functions.name
  location                  = var.location
  project_prefix            = var.project_prefix
  environment               = var.environment
  function_app_url          = azurerm_function_app.main_function_app.default_hostname
  eventgrid_topic_id  = azurerm_eventgrid_topic.etl_events_topic.id 
  storage_account_primary_connection_string = azurerm_storage_account.sadatalake.primary_connection_string
  bronze_container_name              = azurerm_storage_container.container_bronze.name
  silver_container_name              = azurerm_storage_container.container_silver.name
  gold_container_name                = azurerm_storage_container.container_gold.name
  storage_account_name = azurerm_storage_account.sadatalake.name
}


resource "azurerm_role_assignment" "adf_datalake_contributor" {
  scope                = azurerm_storage_account.sadatalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = module.adf.adf_principal_id

  depends_on = [
    azurerm_storage_account.sadatalake,
    module.adf
  ]

  lifecycle {
    ignore_changes = [
      # ignoruj zmiany, które mogą powodować konflikt
      principal_id,
      role_definition_name,
      scope,
    ]
  }
}



# module "databricks" {
#   source                  = "./databricks"

#   # Przekazywanie wymaganych zmiennych do modułu databricks
#   databricks_name         = "${var.project_prefix}-${var.environment}-ws"
#   resource_group_name     = azurerm_resource_group.rg_functions.name # Referencja do RG zdefiniowanego w tym module
#   location                = var.location
#   project_prefix          = var.project_prefix
#   environment             = var.environment

#   # Przekazywanie informacji o Storage Account z modułu głównego do modułu databricks
#   storage_account_name    = azurerm_storage_account.sadatalake.name
#   storage_account_id      = azurerm_storage_account.sadatalake.id
#   bronze_container_name   = azurerm_storage_container.container_bronze.name # Pamiętaj o container_bronze
#   silver_container_name   = azurerm_storage_container.container_silver.name # Pamiętaj o container_bronze

#   key_vault_id            = azurerm_key_vault.main_keyvault.id
#   key_vault_uri           = azurerm_key_vault.main_keyvault.vault_uri
#   azure_data_factory_managed_identity_principal_id = module.adf.adf_principal_id
# }

# resource "azurerm_eventgrid_topic" "ingestion_topic" {
#   name                = "${var.project_prefix}-${var.environment}-ingestion-topic"
#   location            = azurerm_resource_group.rg_functions.location
#   resource_group_name = azurerm_resource_group.rg_functions.name

#   tags = {
#     Environment = var.environment
#     Project     = var.project_prefix
#   }
# }


# resource "azurerm_role_assignment" "function_app_event_grid_publisher" {
#   scope                = azurerm_eventgrid_topic.ingestion_topic.id
#   role_definition_name = "EventGrid Data Sender"
#   principal_id         = azurerm_function_app.main_function_app.identity[0].principal_id
# }