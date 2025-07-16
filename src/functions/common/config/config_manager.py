# src/common/config_manager.py
import os
import json
import logging
from typing import Dict, Optional
# from azure.keyvault.secrets import SecretClient
# from azure.identity import DefaultAzureCredential # Jeśli używasz Managed Identity

logger = logging.getLogger(__name__)

class ConfigManager:
    _instance = None
    _config_cache: Dict[str, str] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        # Preferuj zmienne środowiskowe (czyli App Settings w Azure Functions)
        # Możesz również dodać logikę do odczytu z Azure Key Vault, np.:
        # keyvault_url = os.environ.get("KEY_VAULT_URL")
        # if keyvault_url:
        #    credential = DefaultAzureCredential()
        #    client = SecretClient(vault_url=keyvault_url, credential=credential)
        #    self._config_cache["MY_SECRET"] = client.get_secret("MySecret").value

        for key, value in os.environ.items():
            self._config_cache[key] = value
        
        logger.info("Configuration loaded.")

    def get_setting(self, key: str, default: Optional[str] = None) -> str:
        value = self._config_cache.get(key, default)
        if value is None:
            raise ValueError(f"Configuration setting '{key}' not found.")
        return value