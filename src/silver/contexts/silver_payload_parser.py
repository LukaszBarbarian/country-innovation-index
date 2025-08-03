from src.common.contexts.payload_parser import PayloadParser
from src.silver.contexts.silver_context import SilverContext


class SilverPayloadParser(PayloadParser):
    def parse(self, payload) -> SilverContext:
        main_context = super().parse(payload)

        config_payload = payload.get("processing_config_payload", {})

        required_fields = ["status", "api_name", "dataset_name", "layer_name", "message", "api_response_status_code", "output_path"]

        self._ensure_requires(requires=required_fields, payload=config_payload)

        return SilverContext(
            correlation_id=main_context.correlation_id,
            queue_message_id=main_context.queue_message_id,
            env=main_context.env,
            ingestion_time_utc=main_context.ingestion_time_utc,
            processing_config_payload=main_context.processing_config_payload,
            status=config_payload["status"],
            api_name=config_payload["api_name"],
            dataset_name=config_payload["dataset_name"],
            layer_name=config_payload["layer_name"],
            message=config_payload["message"],
            api_response_status_code=config_payload["api_response_status_code"],
            output_path=config_payload["output_path"],
            error_details=config_payload.get("error_details")
        )
