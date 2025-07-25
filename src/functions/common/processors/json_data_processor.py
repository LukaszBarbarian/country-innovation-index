# src/ingestion/processors/json_data_processor.py

import json
import logging
from datetime import datetime # Pamiętaj o imporcie datetime
from typing import Any # Dodany import dla typowania

from src.functions.common.processors.base_data_processor import BaseDataProcessor
from src.functions.common.models.ingestion_context import IngestionContext
from src.common.storage_account.bronze_storage_manager import BronzeStorageManager


logger = logging.getLogger(__name__)

class JsonDataProcessor(BaseDataProcessor):
    """
    Procesor danych odpowiedzialny za pobieranie odpowiedzi JSON z kontekstu,
    generowanie ścieżki do bloba w warstwie Bronze i zapisywanie danych JSON.
    """

    def __init__(self, storage_manager: BronzeStorageManager):
        """
        Inicjalizuje JsonDataProcessor z menedżerem przechowywania Bronze.

        Args:
            storage_manager (BronzeStorageManager): Instancja menedżera przechowywania Bronze.
        """
        super().__init__(storage_manager)
        logger.info("JsonDataProcessor initialized.")

    def process_and_save(self, context: IngestionContext) -> str:
        """
        Przetwarza surowe dane JSON z kontekstu i zapisuje je do magazynu Bronze.

        Args:
            context (IngestionContext): Obiekt kontekstu zawierający odpowiedź API
                                        i metadane do zapisu.

        Returns:
            str: Pełna ścieżka do zapisanego bloba w magazynie Bronze.

        Raises:
            ValueError: Jeśli odpowiedź API jest nieprawidłowa lub nie jest poprawnym JSON-em.
        """
        logger.info(f"Rozpoczynanie przetwarzania JSON dla {context.api_name}/{context.dataset_name}.")

        # 1. Sprawdzenie i dekodowanie surowych danych API
        if not context.raw_api_response:
            logger.error(f"Brak surowej odpowiedzi API w kontekście dla {context.api_name}/{context.dataset_name}.")
            raise ValueError("Surowa odpowiedź API jest pusta w IngestionContext.")
        
        if context.api_response_status_code != 200:
            logger.warning(f"Odpowiedź API ma status inny niż 200: {context.api_response_status_code} dla {context.api_name}/{context.dataset_name}. Próbuję przetworzyć.")
            # Możesz zdecydować, czy rzucać błąd, czy kontynuować w zależności od przypadku użycia.
            # Dla surowych danych, często chcemy zapisać nawet błędy, aby je przeanalizować.
            # Ale dla danych JSON, jeśli API zwróciło błąd w postaci JSON, to też go zapiszemy.
        
        data_to_save: Any # Definicja typu dla danych do zapisania
        try:
            # Zakładamy, że context.raw_api_response jest obiektem requests.Response
            # i zawiera metodę .json() do parsowania JSON-a.
            data_to_save = context.raw_api_response.json()
            logger.debug(f"Pomyślnie sparsowano JSON z odpowiedzi API dla {context.api_name}/{context.dataset_name}.")
        except AttributeError:
            # Jeśli raw_api_response nie jest obiektem requests.Response (np. jest to już string/bytes)
            try:
                data_to_save = json.loads(context.raw_api_response)
                logger.debug(f"Pomyślnie sparsowano JSON ze stringa/bajtów dla {context.api_name}/{context.dataset_name}.")
            except (json.JSONDecodeError, TypeError) as e:
                logger.error(f"Nie udało się zdekodować JSON z surowej odpowiedzi API dla {context.api_name}/{context.dataset_name}: {e}")
                raise ValueError(f"Odpowiedź API nie jest poprawnym JSON-em dla {context.api_name}: {e}")
        except json.JSONDecodeError as e:
             logger.error(f"Nie udało się zdekodować JSON z obiektu requests.Response dla {context.api_name}/{context.dataset_name}: {e}")
             raise ValueError(f"Odpowiedź API z requests.Response nie jest poprawnym JSON-em dla {context.api_name}: {e}")


        # 2. Generowanie pełnej ścieżki do bloba
        # Używamy timestampu z kontekstu, aby zachować spójność.
        ingestion_datetime = context.ingestion_timestamp if context.ingestion_timestamp else datetime.utcnow()
        timestamp_str = ingestion_datetime.strftime("%Y%m%d%H%M%S")
        year = ingestion_datetime.strftime("%Y")
        month = ingestion_datetime.strftime("%m")
        day = ingestion_datetime.strftime("%d")

        # Określ wzorzec ścieżki dla danych JSON w warstwie Bronze.
        # Ten wzorzec jest specyficzny dla procesora JSON.
        # Może być np. `{api_name}/{dataset_name}/{rok}/{miesiac}/{dzien}/{timestamp}.json`
        # lub `{api_name}/{rok}/{miesiac}/{dzien}/{dataset_name}_{timestamp}.json`
        # Wybieram ten z dataset_name na końcu nazwy pliku, aby był unikalny.
        blob_name_pattern = "{api_name}/{year}/{month}/{day}/{dataset_name}_{timestamp}.json"

        # Formatowanie nazwy bloba
        # Upewnij się, że api_name jest w małych literach dla spójności ścieżek
        blob_name = blob_name_pattern.format(
            api_name=context.api_name.lower(),
            dataset_name=context.dataset_name,
            year=year,
            month=month,
            day=day,
            timestamp=timestamp_str,
        )
        logger.debug(f"Wygenerowana nazwa bloba: {blob_name}")

        # 3. Wywołanie generycznej metody upload_blob w BronzeStorageManager
        # Przekazujemy sparsowany obiekt JSON. `BronzeStorageManager` zajmie się jego serializacją.
        try:
            full_blob_path = self.storage_manager.upload_blob(
                data_content=data_to_save, 
                blob_name=blob_name,
                overwrite=True # Możesz zmienić to na False, jeśli chcesz unikać nadpisywania
            )
            logger.info(f"Pomyślnie zapisano dane JSON dla {context.api_name}/{context.dataset_name} pod ścieżką: {full_blob_path}")
        except Exception as e:
            logger.error(f"Błąd podczas wgrywania danych JSON do storage dla {context.api_name}/{context.dataset_name}: {e}")
            raise # Ponownie rzuć wyjątek, aby orkiestrator mógł go obsłużyć

        # Zapisz pełną ścieżkę bloba z powrotem do kontekstu
        context.set_target_blob_path(full_blob_path)
        
        return full_blob_path