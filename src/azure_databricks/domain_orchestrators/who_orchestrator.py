# azure_databricks/orchestrators/domain_orchestrators/who_orchestrator.py

from azure_databricks.common.orchestrators.base_domain_orchestrator import BaseDomainOrchestrator
from azure_databricks.common.enums.etl_layers import ETLLayer
from azure_databricks.common.orchestrators.domain_orchestrator_registry import DomainOrchestratorRegistry
from azure_databricks.common.enums.domain_source import DomainSource

@DomainOrchestratorRegistry.register_orchestrator_decorator(
    domain_source=DomainSource.WHO,
    applicable_layers=[ETLLayer.BRONZE, ETLLayer.SILVER, ETLLayer.GOLD]
)
class WHOOrchestrator(BaseDomainOrchestrator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def execute(self, target_etl_layer: ETLLayer): # <--- ZMIANA: target_etl_layer
        print(f"\n--- Rozpoczynam przetwarzanie danych dla domeny WHO do warstwy '{target_etl_layer.value}' w środowisku '{self.env}' ---")

        if target_etl_layer == ETLLayer.BRONZE:
            # WHO_Countries - Raw to Bronze
            print("Uruchamiam transformację WHO_Countries: Raw to Bronze...")
            specific_who_countries_config = {
                "read_options": {"header": "true", "inferSchema": "true", "delimiter": ","},
                "partition_cols": ["iso_code"]
            }
            who_countries_bronze_transformer = self.transformer_factory.create_transformer(
                source_id="who_countries", 
                etl_layer=ETLLayer.BRONZE,
                specific_config=specific_who_countries_config
            )
            await who_countries_bronze_transformer.process()
            print("Transformacja WHO_Countries: Raw to Bronze zakończona.")

            # Możesz dodać inne transformacje Raw->Bronze dla WHO tutaj
            # np. WHO_Diseases - Raw to Bronze
            
        elif target_etl_layer == ETLLayer.SILVER:
            # WHO_Countries - Bronze to Silver
            print("Uruchamiam transformację WHO_Countries: Bronze to Silver...")
            specific_who_countries_silver_config = {
                "merge_keys": ["iso_code"] # Przykład dla Silver, jeśli używasz MERGE
            }
            who_countries_silver_transformer = self.transformer_factory.create_transformer(
                source_id="who_countries", 
                etl_layer=ETLLayer.SILVER, # Zmiana na SILVEr
                specific_config=specific_who_countries_silver_config
            )
            await who_countries_silver_transformer.process()
            print("Transformacja WHO_Countries: Bronze to Silver zakończona.")
            
            # Możesz dodać inne transformacje Bronze->Silver dla WHO tutaj
            
        elif target_etl_layer == ETLLayer.GOLD:
            # Tutaj logika transformacji Gold dla WHO
            print(f"Brak zaimplementowanych transformacji GOLD dla WHO w tej chwili.")

        else:
            print(f"Brak zaimplementowanych transformacji dla warstwy '{target_etl_layer.value}' w WHOOrchestrator.")

        print(f"--- Przetwarzanie danych dla domeny WHO do warstwy '{target_etl_layer.value}' zakończone w środowisku '{self.env}' ---")