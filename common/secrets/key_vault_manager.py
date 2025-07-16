import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import threading

class KeyVault:
    _client = None
    _cache = {}
    _lock = threading.Lock()

    @classmethod
    def get_secret(cls, key_name: str) -> str:
        if key_name in cls._cache:
            return cls._cache[key_name]

        if not cls._client:
            vault_name = os.getenv("KEY_VAULT_NAME")
            cls._client = SecretClient(
                vault_url=f"https://{vault_name}.vault.azure.net/",
                credential=DefaultAzureCredential()
            )

        secret = cls._client.get_secret(key_name)
        cls._cache[key_name] = secret.value
        return secret.value
