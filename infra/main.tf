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

## --- Pakowanie i Wdrażanie Kodu Funkcji ---

# ### 6. Null Resource do pakowania kodu funkcji za pomocą skryptu Python
# resource "null_resource" "zip_function_code_package" {
#   # `local-exec` uruchamia komendę na maszynie, z której uruchamiasz Terraform
#   provisioner "local-exec" {
#     # Upewnij się, że masz Pythona w PATH lub podaj pełną ścieżkę do interpretera Pythona.
#     # To polecenie wywołuje Twój skrypt Python, przekazując mu ścieżkę źródłową kodu i ścieżkę wyjściowego ZIPa.
#     command = "python ${local.create_zip_script_path} ${local.source_code_dir} ${local.output_zip_file}"
#     # Jeśli Python nie jest w PATH, możesz spróbować:
#     # interpreter = ["cmd.exe", "/C"] # Dla Windowsa
#     # interpreter = ["bash", "-c"]   # Dla Linux/macOS
#   }

#   # `triggers` sprawia, że provisioner uruchomi się tylko wtedy, gdy zmieni się hasz kodu źródłowego.
#   # Hasz jest obliczany na podstawie zawartości wszystkich plików w katalogu source_code_dir.
#   triggers = {
#     source_code_hash = md5(join("", [
#       for f in fileset(local.source_code_dir, "**") : filemd5(
#         # Ważne: Zbuduj pełną ścieżkę do pliku, aby filemd5 mogło go znaleźć
#         format("%s/%s", local.source_code_dir, f)
#       )
#     ]))
#   }
# }

### 7. Null Resource do wdrożenia kodu funkcji za pomocą Azure CLI
# resource "null_resource" "deploy_function_code_cli" {
#   depends_on = [
#     azurerm_function_app.main_function_app, # Zapewnia, że Function App jest już utworzona
#     null_resource.zip_function_code_package # Zapewnia, że plik ZIP został utworzony i jest aktualny
#   ]

#   provisioner "local-exec" {
#     # Używamy Azure CLI do wdrożenia wcześniej utworzonego pliku ZIP.
#     # Musisz mieć zainstalowane Azure CLI i być zalogowany.
#     command = <<EOT
# az functionapp deployment source config-zip --resource-group ${azurerm_resource_group.rg_functions.name} --name ${azurerm_function_app.main_function_app.name} --src ${local.output_zip_file}
# az functionapp restart --resource-group ${azurerm_resource_group.rg_functions.name} --name ${azurerm_function_app.main_function_app.name}
# EOT
#     # Użyj odpowiedniego interpretera w zależności od systemu operacyjnego
#     # Dla Windowsa:
#     interpreter = ["cmd.exe", "/C"]
#     # Dla Linux/macOS:
#     # interpreter = ["bash", "-c"]
#   }

#   # Ten triggers blokuje wykonanie provisionera, dopóki hash kodu źródłowego się nie zmieni.
#   # Odwołujemy się do hasha wygenerowanego przez 'zip_function_code_package',
#   # co oznacza, że wdrożenie nastąpi tylko wtedy, gdy kod funkcji ulegnie zmianie.
#   triggers = {
#     source_code_hash = null_resource.zip_function_code_package.triggers.source_code_hash
#   }
# }
