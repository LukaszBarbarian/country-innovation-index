from src.common.contexts.payload_parser import PayloadParser
from src.silver.contexts.silver_context import SilverContext


class SilverPayloadParser(PayloadParser):
    def parse(self, payload) -> SilverContext:
        main_context = super().parse(payload)

        config_payload = payload.get("processing_config_payload", {})

        required_fields = [
            "status", "apiName", "datasetName", "layerName", "message",
            "apiResponseStatusCode", "outputPath"
        ]

        self._ensure_requires(requires=required_fields, payload=config_payload)

        return SilverContext(
            correlation_id=main_context.correlation_id,
            queue_message_id=main_context.queue_message_id,
            env=main_context.env,
            ingestion_time_utc=main_context.ingestion_time_utc,
            processing_config_payload=main_context.processing_config_payload,
            status=config_payload["status"],
            api_name=config_payload["apiName"],
            dataset_name=config_payload["datasetName"],
            layer_name=config_payload["layerName"],
            message=config_payload["message"],
            api_response_status_code=config_payload["apiResponseStatusCode"],
            output_path=config_payload["outputPath"],
            error_details=config_payload.get("errorDetails")
        )
