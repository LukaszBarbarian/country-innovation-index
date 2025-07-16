# bronze_ingestion/api_client/base.py

import requests
from abc import ABC, abstractmethod
from typing import Dict, Optional, Any
import logging

logger = logging.getLogger(__name__) # Logger dla modułu base.py

class ApiResponse:
    """
    Standardowa klasa odpowiedzi dla wywołań API, opakowująca surową odpowiedź requests.
    """
    def __init__(self, raw: requests.Response):
        self.status_code = raw.status_code
        self.headers = raw.headers
        self.text = raw.text
        self.ok = raw.ok  # Dodajemy atrybut 'ok' z requests.Response
        self.raw_response = raw # Zachowaj surową odpowiedź requests dla celów debugowania/rozszerzeń

        try:
            self.data = raw.json()
        except requests.exceptions.JSONDecodeError: # Użyj specyficznego wyjątku dla JSON
            self.data = None
            if self.ok: # Jeśli status OK, a JSON jest pusty/niepoprawny, to ostrzeżenie
                logger.warning(f"ApiResponse: Could not decode JSON from response with status {self.status_code}. Response text: {raw.text[:100]}...")
        except ValueError: # Stara wersja wyjątku, dla kompatybilności wstecznej (mniej precyzyjna)
            self.data = None
            if self.ok:
                logger.warning(f"ApiResponse: Could not decode JSON (ValueError) from response with status {self.status_code}. Response text: {raw.text[:100]}...")


    def json(self) -> Any:
        """Zwraca sparsowane dane JSON."""
        return self.data

    def __str__(self):
        return f"ApiResponse(status={self.status_code}, ok={self.ok})"

    def raise_for_status(self):
        """
        Podnosi wyjątek HTTPError, jeśli status odpowiedzi wskazuje na błąd klienta lub serwera.
        Przydatne, jeśli chcesz, aby błędy były obsługiwane wyżej w stosie wywołań.
        """
        if self.raw_response:
            self.raw_response.raise_for_status()
        elif not self.ok:
            raise requests.exceptions.RequestException(
                f"API call failed with status {self.status_code} (no raw response to raise_for_status)."
            )


class ApiClient(ABC):
    """
    Abstrakcyjna klasa bazowa dla wszystkich klientów API.
    """
    # Usunięto base_url jako atrybut klasy, będzie w konstruktorze instancji

    def __init__(self, base_url: str, timeout: int = 10): # base_url jest teraz wymagany w konstruktorze
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug(f"ApiClient initialized with base_url: {self.base_url}")

    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> ApiResponse:
        """
        Wykonuje zapytanie GET do API.
        Zawsze zwraca obiekt ApiResponse, nawet w przypadku błędów HTTP/sieciowych.
        """
        url = self._build_url(endpoint)
        self.logger.info(f"Making GET request to: {url}, params={params}")
        
        try:
            response = self.session.get(
                url, params=params, headers=headers, timeout=self.timeout
            )
            # Nie wywołujemy raise_for_status() bezpośrednio tutaj,
            # aby zawsze zwrócić ApiResponse i umożliwić elastyczną obsługę błędów.
            # Kod wywołujący może sam wywołać ApiResponse.raise_for_status() jeśli potrzebuje.
            return ApiResponse(response)
        except requests.exceptions.HTTPError as http_err:
            # W przypadku błędu HTTP (np. 404, 500), requests.exceptions.HTTPError jest już w raw_response.
            # ApiResponse obsłuży status i ustawi 'ok' na False.
            self.logger.error(f"HTTP Error during GET request to {url}: {http_err}. Status: {http_err.response.status_code if http_err.response else 'N/A'}")
            # Zwróć ApiResponse nawet w przypadku błędu HTTP, opakowując odpowiedź
            return ApiResponse(http_err.response) if http_err.response else \
                   ApiResponse(type("obj", (object,), {"status_code": 500, "headers": {}, "text": str(http_err), "ok": False})()) # Mock object for no response

        except requests.exceptions.RequestException as req_err:
            # W przypadku innych błędów żądania (np. połączenia, timeout)
            self.logger.error(f"Request Error during GET request to {url}: {req_err}")
            # Stwórz ApiResponse z informacją o błędzie połączenia/requestu
            return ApiResponse(type("obj", (object,), {"status_code": 503, "headers": {}, "text": str(req_err), "ok": False})())
            # ^ Tworzenie prostego mocka obiektu requests.Response, gdy nie ma faktycznej odpowiedzi HTTP
        except Exception as e:
            # Catch all innych nieoczekiwanych błędów
            self.logger.critical(f"An unexpected error occurred during GET request to {url}: {e}", exc_info=True)
            return ApiResponse(type("obj", (object,), {"status_code": 500, "headers": {}, "text": str(e), "ok": False})())


    def _build_url(self, endpoint: str) -> str:
        """
        Buduje pełny URL na podstawie bazowego URL i endpointu.
        """
        return f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"

    @abstractmethod
    def fetch_data(self, **kwargs) -> ApiResponse:
        """
        Abstrakcyjna metoda do pobierania danych z API.
        Każda konkretna implementacja klienta API musi zaimplementować tę metodę,
        określając swoje specyficzne argumenty (**kwargs).
        """
        pass