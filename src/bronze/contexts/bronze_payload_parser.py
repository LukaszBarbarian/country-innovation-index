from src.common.contexts.payload_parser import PayloadParser
from src.bronze.contexts.bronze_context import BronzeContext
from src.common.enums.domain_source import DomainSource
from typing import Any
from datetime import datetime

class BronzePayloadParser(PayloadParser):
    def parse(self, payload):
        main_context = super().parse(payload)

        if "api_config_payload" not in payload:
            raise ValueError("Payload must contain 'api_config_payload' key.")
        
        api_config_payload_from_input = payload["api_config_payload"]

        self._ensure_requires(requires=["api_name", "dataset_name"], payload=api_config_payload_from_input)

        correlation_id = main_context.correlation_id
        queue_message_id = main_context.queue_message_id
        etl_layer = main_context.etl_layer
        env = main_context.env
        
        api_name_str = api_config_payload_from_input["api_name"]
        dataset_name = api_config_payload_from_input["dataset_name"]
        api_request_payload = api_config_payload_from_input.get("api_request_payload", {})


        ctx = BronzeContext(
            api_name_str=api_name_str,
            dataset_name=dataset_name,
            api_request_payload=api_request_payload,
            correlation_id=correlation_id,
            queue_message_id=queue_message_id,
            api_config_payload=api_config_payload_from_input,
            ingestion_date_utc=datetime.utcnow(),
            env=env)
        
        ctx.domain_source = self._map_api_name_to_domain_source(api_name_str=api_name_str)
        
        return ctx
    


    def _map_api_name_to_domain_source(self, api_name_str: str) -> DomainSource:
        """
        Maps an API name string to a DomainSource enum member.
        Private method for internal use during initialization.
        """
        try:
            domain_source_member = DomainSource(api_name_str)
            return domain_source_member
        except ValueError:
            raise ValueError(f"Invalid API name: '{api_name_str}'. Expected one of: {[e.value for e in DomainSource]}")
        except Exception as e:
            raise RuntimeError(f"An unexpected error occurred during API name mapping: {e}")

