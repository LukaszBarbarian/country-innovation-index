# src/common/config_manager.py
import os
import json
import logging
from typing import Dict, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

class ConfigManager:
    _instance = None
    _config_cache: Dict[str, str] = {}
    _local_settings_loaded = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        for key, value in os.environ.items():
            self._config_cache[key.upper()] = value
        logger.info("Configuration loaded from environment.")

    def _try_load_from_local_settings(self, key: str, settings_path: Optional[str] = None):
        if self._local_settings_loaded:
            return  # nie próbuj ładować drugi raz

        if settings_path is None:
            # domyślnie 3 poziomy wyżej niż aktualny katalog roboczy
            project_root = Path.cwd()
            for parent in [project_root] + list(project_root.parents):
                potential = parent / "local.settings.json"
                if potential.exists():
                    settings_path = potential
                    break
            else:
                logger.warning("local.settings.json not found.")
                return

        try:
            with open(settings_path, "r") as f:
                data = json.load(f)
                for k, v in data.get("Values", {}).items():
                    key_upper = k.upper()
                    if key_upper not in self._config_cache:
                        self._config_cache[key_upper] = v
                self._local_settings_loaded = True
                logger.info(f"Loaded settings from {settings_path}")
        except Exception as e:
            logger.warning(f"Failed to load local.settings.json: {e}")

    def get_setting(self, key: str, default: Optional[str] = None, try_local_settings: bool = True) -> str:
        key_upper = key.upper()
        value = self._config_cache.get(key_upper)

        if value is None and try_local_settings:
            self._try_load_from_local_settings(key)
            value = self._config_cache.get(key_upper)

        if value is None:
            if default is not None:
                return default
            raise ValueError(f"Configuration setting '{key}' not found.")

        return value
