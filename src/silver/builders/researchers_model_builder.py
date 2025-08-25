from typing import Dict, Tuple
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from src.common.enums.domain_source import DomainSource
from src.common.enums.model_type import ModelType
from src.common.registers.model_builder_registry import ModelBuilderRegistry
from src.silver.builders.silver_model_builder import SilverModelBuilder


@ModelBuilderRegistry.register(ModelType.RESEARCHERS)
class ResearchersModelBuilder(SilverModelBuilder):
    async def build(
        self,
        datasets: Dict[Tuple, DataFrame],
        dependencies: Dict[ModelType, DataFrame]
    ) -> DataFrame:
        country_df = dependencies.get(ModelType.COUNTRY)
        if country_df is None or country_df.count() == 0:
            raise ValueError("Brak modelu krajów (COUNTRY) w zależnościach.")

        # 2. Pobranie ramki danych o bezrobociu z datasetów
        researchers_df = datasets.get((DomainSource.WORLDBANK, "researchers"))
        if not researchers_df or researchers_df.count() == 0:
            raise ValueError("Brak danych w ramce danych 'researchers'.")

        # 3. Znormalizuj kolumny danych o bezrobociu
        # Zgodnie z WorldBankTransformer, dane wejściowe powinny zawierać kolumny 'ISO3166-1-Alpha-3'
        # oraz 'unemployment_rate'.
        researchers_df = researchers_df.select(
            F.col("ISO3166-1-Alpha-3"),
            F.col("year"),
            F.col("value")
        )

        # 4. Join z modelem krajów po ISO
        final_df = researchers_df.join(
            country_df,
            researchers_df["ISO3166-1-Alpha-3"] == country_df["ISO3166-1-Alpha-3"],
            "inner"  # inner join zwraca tylko kraje z referencji
        ).select(
            researchers_df["ISO3166-1-Alpha-3"].alias("ISO3166-1-Alpha-3"),
            F.col("year"),
            F.col("value"),
            F.col("country_name"),
            F.col("ref_worldbank")  # flaga informująca, że kraj występuje w World Bank
        )

        return final_df