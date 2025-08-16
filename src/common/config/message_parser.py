import datetime
from typing import Any, Dict
from src.common.models.message import Message

class MessageParser:
    """
    Parser do wiadomości o różnej strukturze.
    """
    def parse(self, message_data: Dict[str, Any]) -> Message:
        """
        Parsuje słownik na obiekt klasy Message.

        Args:
            message_data: Słownik zawierający dane wiadomości.

        Returns:
            Obiekt Message.

        Raises:
            ValueError: Jeśli dane są nieprawidłowe lub brakuje wymaganych pól.
        """
        try:
            parsed_data = message_data.copy()

            date_str = parsed_data.pop("message_date")
            parsed_data["message_date"] = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M")

            return Message(**parsed_data)
        
        except (KeyError, ValueError) as e:
            raise ValueError(f"Błąd podczas parsowania wiadomości: {e}")