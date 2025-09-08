# src/silver/model_builders/patents_model_builder.py
import pyspark.sql.functions as F
from typing import Dict, Tuple
from pyspark.sql import DataFrame
from injector import inject

from src.common.enums.model_type import ModelType
from src.common.enums.domain_source import DomainSource
from src.common.registers.model_builder_registry import ModelBuilderRegistry
from src.silver.builders.silver_model_builder import SilverModelBuilder


# src/silver/model_builders/patents_model_builder.py
import pyspark.sql.functions as F
from typing import Dict, Tuple
from pyspark.sql import DataFrame
from injector import inject

from src.common.enums.model_type import ModelType
from src.common.enums.domain_source import DomainSource
from src.common.registers.model_builder_registry import ModelBuilderRegistry
from src.silver.builders.silver_model_builder import SilverModelBuilder


@ModelBuilderRegistry.register(ModelType.PATENTS)
class PatentsModelBuilder(SilverModelBuilder):
    async def build(
        self,
        datasets: Dict[Tuple, DataFrame],
        dependencies: Dict[ModelType, DataFrame]
    ) -> DataFrame:
        patents_df = datasets.get((DomainSource.PATENTS, "patents"))
        if not patents_df:
            raise ValueError("Brak danych źródłowych dla 'patents'.")

        country_df = dependencies.get(ModelType.COUNTRY)
        if country_df is None:
            raise ValueError("Brak zależności: model 'country'.")

        # 🔹 agregacja po Origin (=country_code) i Year
        aggregated_df = (
            patents_df.groupBy("country_code", "year")
            .agg(
                F.sum("patents_count").alias("patents_total"),
                F.sum("resident_patents").alias("resident_patents"),
                F.sum("abroad_patents").alias("abroad_patents")
            )
        )

        # 🔹 join do country, żeby mieć ISO3166-1-Alpha-3
        result_df = aggregated_df.join(
            country_df,
            aggregated_df["country_code"] == country_df["country_code"],
            "inner"
        )

        # 🔹 selekcja kolumn
        final_df = result_df.select(
            country_df["country_name"],
            country_df["ISO3166-1-Alpha-3"],
            aggregated_df["year"],
            aggregated_df["patents_total"],
            aggregated_df["resident_patents"],
            aggregated_df["abroad_patents"]
        )

        return final_df