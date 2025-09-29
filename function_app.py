import datetime
import logging
import traceback
from typing import Any, Dict
import uuid
import azure.functions as func
import azure.durable_functions as df

from src.bronze.contexts.bronze_parser import BronzeParser
from src.common.azure_clients.event_grid_client_manager import EventGridNotifier
from src.common.factories.orchestrator_factory import OrchestratorFactory
from src.common.config.config_manager import ConfigManager
from src.common.enums.etl_layers import ETLLayer
from src.bronze.init import bronze_init
from src.common.exceptions.exception import (
    IngestionError,
    InvalidPayloadError,
    OrchestratorExecutionError,
    NotificationError,
)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
logger = logging.getLogger(__name__)


def handle_exception(e: Exception, correlation_id: str, fail_hard: bool = True) -> Dict[str, Any]:
    error_dict = {
        "status": "FAILED",
        "correlation_id": correlation_id,
        "message": str(e),
        "error_type": type(e).__name__,
        "error_details": traceback.format_exc(),
    }
    logger.exception(f"Failure for correlation {correlation_id}: {e}")

    if fail_hard:
        raise RuntimeError(f"Ingestion failed: {error_dict}")
    return error_dict


@app.route(route="start_ingestion")
@app.durable_client_input(client_name="starter")
async def start_ingestion_http(req: func.HttpRequest, starter: df.DurableOrchestrationClient) -> func.HttpResponse:
    """
    HTTP trigger to start the data ingestion orchestration.
    """
    try:
        manifest_payload = req.get_json()
    except ValueError:
        logger.error("Request does not contain valid JSON.")
        return func.HttpResponse("Error: Expected a valid JSON format.", status_code=400)
    except Exception as e:
        logger.exception(f"Failed to process request: {e}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

    try:
        instance_id = await starter.start_new("ingest_orchestrator", None, manifest_payload)
        logger.info(f"Started orchestration with ID = '{instance_id}'.")
        return starter.create_check_status_response(req, instance_id)
    except Exception as e:
        logger.exception(f"Failed to start orchestration: {e}")
        return func.HttpResponse(f"Error starting orchestration: {e}", status_code=500)


@app.orchestration_trigger(context_name="context")
def ingest_orchestrator(context: df.DurableOrchestrationContext):
    """
    Durable orchestration to manage the bronze layer data ingestion process.
    """
    input_payload = context.get_input()

    if isinstance(input_payload, list) and len(input_payload) == 1:
        input_payload = input_payload[0]

    if not isinstance(input_payload, dict) or not input_payload:
        logger.error("Orchestrator received an empty or invalid payload.")
        return {
            "status": "FAILED",
            "message": "Orchestrator received an empty or invalid payload."
        }

    orchestrator_result_dict = yield context.call_activity(
        "run_ingestion_activity",
        {"input_payload": input_payload}
    )

    logger.info(f"Bronze orchestration completed. Result: {orchestrator_result_dict.get('status', 'N/A')}")
    return orchestrator_result_dict


@app.activity_trigger(input_name="input")
async def run_ingestion_activity(input: Dict[str, Any]) -> Dict[str, Any]:
    """
    Activity function to execute the bronze layer data ingestion logic AND
    publish an event to Event Grid.
    """
    config = ConfigManager()
    input_payload = input.get("input_payload", {})

    correlation_id = input_payload.get("correlation_id", str(uuid.uuid4()))
    logger.info(f"Running ingestion for correlation ID: {correlation_id}")

    try:
        orchestrator = OrchestratorFactory.get_instance(ETLLayer.BRONZE, config=config)
        parser = BronzeParser(config)

        try:
            bronze_context = parser.parse(input_payload)
            if not bronze_context:
                raise InvalidPayloadError("BronzeParser returned empty context", correlation_id)
            bronze_context.correlation_id = correlation_id
        except Exception as e:
            raise InvalidPayloadError(f"Failed to parse input payload: {e}", correlation_id)

        try:
            result = await orchestrator.execute(bronze_context)
        except Exception as e:
            raise OrchestratorExecutionError(f"Orchestrator execution failed: {e}", correlation_id)

        result_dict = result.to_dict()

        try:
            notifier = EventGridNotifier(config.get("EVENT_GRID_ENDPOINT"), config.get("EVENT_GRID_KEY"))
            silver_manifest_path = f"/silver/manifest/{input_payload.get('env')}.manifest.json"

            event_grid_payload = {
                "layer": ETLLayer.BRONZE.value,
                "env": input_payload.get("env"),
                "status": result.status,
                "message_date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "correlation_id": result.correlation_id,
                "manifest": silver_manifest_path,
                "summary_ingestion_uri": result.summary_url,
                "duration_in_ms": result.duration_in_ms,
            }

            notifier.send_notification(
                ETLLayer.BRONZE.value,
                "BronzeIngestionCompleted",
                event_grid_payload,
                result.correlation_id,
            )
            logger.info(f"Event Grid notification sent successfully for correlation ID {correlation_id}")

        except Exception as eg:
            raise NotificationError(f"Event Grid notification failed: {eg}", correlation_id)

        return result_dict

    except Exception as e:
        return handle_exception(e, correlation_id, fail_hard=True)
