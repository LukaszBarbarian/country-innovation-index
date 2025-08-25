from typing import Dict, Tuple
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from src.common.enums.domain_source import DomainSource
from src.common.enums.model_type import ModelType
from src.common.registers.model_builder_registry import ModelBuilderRegistry
from src.silver.builders.silver_model_builder import SilverModelBuilder


@ModelBuilderRegistry.register(ModelType.PKB)
class PKBModelBuilder(SilverModelBuilder):
    async def build(
        self,
        datasets: Dict[Tuple, DataFrame],
        dependencies: Dict[ModelType, DataFrame]
    ) -> DataFrame:
        country_df = dependencies.get(ModelType.COUNTRY)
        if country_df is None or country_df.count() == 0:
            raise ValueError("Brak modelu krajów (COUNTRY) w zależnościach.")

        # 2. Pobranie ramki danych o bezrobociu z datasetów
        pkb_df = datasets.get((DomainSource.WORLDBANK, "pkb"))
        if not pkb_df or pkb_df.count() == 0:
            raise ValueError("Brak danych w ramce danych 'pkb'.")

        # 3. Znormalizuj kolumny danych o bezrobociu
        # Zgodnie z WorldBankTransformer, dane wejściowe powinny zawierać kolumny 'ISO3166-1-Alpha-3'
        # oraz 'unemployment_rate'.
        pkb_df = pkb_df.select(
            F.col("ISO3166-1-Alpha-3"),
            F.col("year"),
            F.col("value")
        )

        # 4. Join z modelem krajów po ISO
        final_df = pkb_df.join(
            country_df,
            pkb_df["ISO3166-1-Alpha-3"] == country_df["ISO3166-1-Alpha-3"],
            "inner"  # inner join zwraca tylko kraje z referencji
        ).select(
            pkb_df["ISO3166-1-Alpha-3"].alias("ISO3166-1-Alpha-3"),
            F.col("year"),
            F.col("value"),
            F.col("country_name"),
            F.col("ref_worldbank")  # flaga informująca, że kraj występuje w World Bank
        )

        return final_df