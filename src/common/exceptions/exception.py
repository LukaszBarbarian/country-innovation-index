class IngestionError(Exception):
    """Base exception for ingestion process errors."""
    def __init__(self, message: str, correlation_id: str = None, payload: dict = None):
        self.correlation_id = correlation_id
        self.payload = payload or {}
        super().__init__(message)



class InvalidPayloadError(IngestionError):
    """Raised when the input payload is invalid."""
    pass

class OrchestratorExecutionError(IngestionError):
    """Raised when the orchestrator fails during execution."""
    pass

class NotificationError(IngestionError):
    """Raised when Event Grid notification fails."""
    pass        