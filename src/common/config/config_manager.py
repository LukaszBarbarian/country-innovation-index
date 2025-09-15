import os
import json
from azure.identity import DefaultAzureCredential
from azure.appconfiguration import AzureAppConfigurationClient
from azure.keyvault.secrets import SecretClient
#from urllib.parse import urlparse

class ConfigManager:
    """
    Manages application configuration, retrieving settings from Azure App Configuration
    and automatically resolving references to secrets stored in Azure Key Vault.
    """
    def __init__(self, app_config_endpoint: str = "https://demosur-dev-appconf.azconfig.io"):
        """
        Initializes the ConfigManager with the Azure App Configuration client.

        Args:
            app_config_endpoint (str): The endpoint URL of the Azure App Configuration instance.
                                       Defaults to a development endpoint.
        """
        self.credential = DefaultAzureCredential()
        self.app_config_client = AzureAppConfigurationClient(app_config_endpoint, self.credential)
        self.keyvault_clients = {}

    def get(self, key: str) -> str:
        """
        Retrieves a value from App Configuration, resolving Key Vault references if needed.

        Args:
            key (str): The key of the configuration setting to retrieve.

        Returns:
            str: The value of the configuration setting.
        """
        setting = self.app_config_client.get_configuration_setting(key=key)

        # If it's a Key Vault reference
        if setting.content_type and setting.content_type.startswith(
            "application/vnd.microsoft.appconfig.keyvaultref"
        ):
            ref = json.loads(setting.value)
            secret_uri = ref["uri"]
            return self._get_secret(secret_uri)

        # It's a plain value
        return setting.value

    def _get_secret(self, secret_uri: str) -> str:
        """
        Retrieves a secret from a Key Vault based on its full URI.

        This method caches Key Vault clients to avoid re-initializing them for
        multiple calls to the same vault.

        Args:
            secret_uri (str): The full URI of the secret in Azure Key Vault.

        Returns:
            str: The value of the secret.
        """
        #parsed = urlparse(secret_uri)
        vault_url = f"{parsed.scheme}://{parsed.netloc}"

        if vault_url not in self.keyvault_clients:
            self.keyvault_clients[vault_url] = SecretClient(vault_url=vault_url, credential=self.credential)

        client = self.keyvault_clients[vault_url]
        secret_name = parsed.path.split("/")[2]
        secret_version = parsed.path.split("/")[3] if len(parsed.path.split("/")) > 3 else None

        return client.get_secret(secret_name, secret_version).value