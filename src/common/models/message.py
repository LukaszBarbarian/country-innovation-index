from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Message:
    """
    Klasa reprezentująca wiadomość o statusie przetwarzania.
    """
    layer: str
    status: str
    env: str
    message_date: datetime
    correlation_id: str
    manifest: str
    bronzeOutputUri: Optional[str] = field(default=None)