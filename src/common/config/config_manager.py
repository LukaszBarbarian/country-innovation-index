import os
import json
from azure.identity import DefaultAzureCredential
from azure.appconfiguration import AzureAppConfigurationClient
from azure.keyvault.secrets import SecretClient
from urllib.parse import urlparse

class ConfigManager:
    def __init__(self, app_config_endpoint: str = "https://demosur-dev-appconf.azconfig.io"):
        """
        app_config_endpoint: np. "https://<twoja_nazwa>.azconfig.io"
        """
        self.credential = DefaultAzureCredential()
        self.app_config_client = AzureAppConfigurationClient(app_config_endpoint, self.credential)
        self.keyvault_clients = {}

    def get(self, key: str) -> str:
        """Pobiera wartość z App Configuration (rozwija referencje do Key Vault)."""
        setting = self.app_config_client.get_configuration_setting(key=key)

        # Jeśli to Key Vault reference
        if setting.content_type and setting.content_type.startswith(
            "application/vnd.microsoft.appconfig.keyvaultref"
        ):
            ref = json.loads(setting.value)
            secret_uri = ref["uri"]
            return self._get_secret(secret_uri)

        # Wartość jawna
        return setting.value

    def _get_secret(self, secret_uri: str) -> str:
        """Pobiera sekret z Key Vault na podstawie pełnego URI."""
        parsed = urlparse(secret_uri)
        vault_url = f"{parsed.scheme}://{parsed.netloc}"

        if vault_url not in self.keyvault_clients:
            self.keyvault_clients[vault_url] = SecretClient(vault_url=vault_url, credential=self.credential)

        client = self.keyvault_clients[vault_url]
        secret_name = parsed.path.split("/")[2]
        secret_version = parsed.path.split("/")[3] if len(parsed.path.split("/")) > 3 else None

        return client.get_secret(secret_name, secret_version).value
