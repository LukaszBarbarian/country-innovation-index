# src/silver/model_builders/country_model_builder.py

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from src.common.builders.base_model_builder import BaseModelBuilder
from src.common.enums.model_type import ModelType
from src.common.enums.domain_source import DomainSource # Potrzebne do identyfikacji źródeł
from typing import List, Type, Dict
from src.common.registers.model_builder_registry import ModelBuilderRegistry


@ModelBuilderRegistry.register(ModelType.COUNTRY)
class CountryModelBuilder(BaseModelBuilder):
    """
    ModelBuilder dla encji 'Kraju' w warstwie Silver.
    Odpowiedzialny za pobieranie danych o krajach z różnych źródeł Bronze,
    ich standaryzację, unifikację i tworzenie spójnego modelu kraju.
    """

    def get_model_type(self) -> ModelType:
        """Zwraca typ modelu, który ten builder tworzy."""
        return ModelType.COUNTRY

    def get_silver_table_name(self) -> str:
        """Zwraca nazwę tabeli Delta Lake dla modelu Country."""
        return "countries"

    def build_model(self) -> DataFrame:
        """
        Główna logika budowania modelu Country.
        1. Pobiera zarejestrowane readery dla ModelType.COUNTRIES.
        2. Czyta dane z każdego źródła (Bronze/Bronze Plus).
        3. Standaryzuje kolumny dla każdego źródła.
        4. Scala wszystkie standaryzowane dane.
        5. Wykonuje unifikację i deduplikację, tworząc finalny model.
        """
        self.spark.sparkContext.setJobGroup(self.get_model_type().name, "Building Silver Country Model")
        print(f"[{self.get_model_type().name}] Rozpoczynanie budowania modelu...")

        all_raw_country_data: List[DataFrame] = []

        # 1. Pobieramy wszystkie zarejestrowane readery dla modelu COUNTRIES
        # Dzieje się to automatycznie dzięki BronzeReaderRegistry.get_readers_for_model
        country_reader_classes: List[Type[BaseBronzeReader]] = \
            BronzeReaderRegistry.get_readers_for_model(ModelType.COUNTRIES)

        if not country_reader_classes:
            raise ValueError(f"Brak zarejestrowanych readerów Bronze dla ModelType.{ModelType.COUNTRIES.name}. "
                             "Sprawdź, czy odpowiednie readery są importowane i dekorowane.")

        for reader_class in country_reader_classes:
            reader_instance = reader_class(self.spark) # Tworzymy instancję readera
            source_domain = reader_instance.get_source_domain()
            print(f"[{self.get_model_type().name}] Czytam dane z {source_domain.name} za pomocą {reader_class.__name__}...")
            
            source_df = reader_instance.read_data()
            
            # 2. Standaryzujemy dane dla konkretnego źródła
            standardized_df = self._standardize_source_data(source_df, source_domain)
            all_raw_country_data.append(standardized_df)
            print(f"[{self.get_model_type().name}] Dane z {source_domain.name} standaryzowane. Wierszy: {standardized_df.count()}")


        # 3. Scalamy wszystkie DataFrames z różnych źródeł
        if not all_raw_country_data:
            print(f"[{self.get_model_type().name}] Brak danych do scalenia. Zwracam pusty DataFrame.")
            # Zwróć pusty DataFrame z oczekiwanym schematem, aby uniknąć błędów schematu
            return self.spark.createDataFrame([], schema=self._get_final_schema())

        # Używamy unionByName, aby poprawnie scalać DFy z różnymi kolumnami (allowMissingColumns=True)
        unified_countries_df = all_raw_country_data[0]
        for df in all_raw_country_data[1:]:
            unified_countries_df = unified_countries_df.unionByName(df, allowMissingColumns=True)
        
        print(f"[{self.get_model_type().name}] Wszystkie dane scalone. Całkowita liczba wierszy: {unified_countries_df.count()}")

        # 4. Unifikacja i deduplikacja dla wszystkich źródeł
        final_countries_df = self._unify_and_deduplicate(unified_countries_df)
        
        print(f"[{self.get_model_type().name}] Finalny model Country zbudowany. Liczba unikalnych krajów: {final_countries_df.count()}")

        return final_countries_df

    def _standardize_source_data(self, df: DataFrame, source_domain: DomainSource) -> DataFrame:
        """
        Wstępne standaryzowanie danych z konkretnego źródła do wspólnego formatu.
        Mapuje specyficzne dla źródła nazwy kolumn na ustandaryzowane nazwy
        i wykonuje początkowe czyszczenie/rozpakowywanie.
        """
        # Dodaj kolumnę SourceSystem
        df = df.withColumn("SourceSystem", F.lit(source_domain.name))

        if source_domain == DomainSource.NOBEL_PRIZES:
            # Zakładamy, że NobelCountriesReader zwraca DF z kolumną 'country_name'
            df = df.select(
                F.col("country_name").alias("CountryNameRaw"),
                F.lit(None).cast(StringType()).alias("ISOCode2Raw"), # Brak ISO w Nobelu
                F.col("SourceSystem")
            )
            # Dalsze czyszczenie specyficzne dla NOBEL (np. usunięcie duplikatów z array, trim, lower)
            df = df.filter(F.col("CountryNameRaw").isNotNull() & (F.length(F.trim(F.col("CountryNameRaw"))) > 0)) \
                   .withColumn("CountryNameRaw", F.trim(F.col("CountryNameRaw")))
                   
        elif source_domain == DomainSource.WHO:
            # Zakładamy, że WHOCountriesReader zwraca DF z kolumnami 'country_name', 'iso_code_2'
            df = df.select(
                F.col("country_name").alias("CountryNameRaw"),
                F.col("iso_code_2").alias("ISOCode2Raw"),
                F.col("SourceSystem")
            )
            # Dalsze czyszczenie specyficzne dla WHO
            df = df.filter(F.col("CountryNameRaw").isNotNull() & (F.length(F.trim(F.col("CountryNameRaw"))) > 0)) \
                   .withColumn("CountryNameRaw", F.trim(F.col("CountryNameRaw")))
        else:
            raise ValueError(f"[{self.get_model_type().name}] Nieznane źródło danych do standaryzacji: {source_domain.name}")

        # Mapowanie niespójnych nazw krajów (np. "USA" -> "United States")
        # Możesz to rozszerzyć do większej tabeli mapowań w konfiguracji
        country_name_mapping = self.spark.createDataFrame([
            ("USA", "United States"),
            ("United Kingdom", "United Kingdom"), # Upewnij się, że wspólne nazwy też są w mapowaniu jeśli to lookup
            ("France", "France"),
            ("Poland", "Poland"),
            ("Germany", "Germany")
            # Dodaj więcej mapowań w miarę potrzeb
        ], ["CountryNameRaw", "CountryNameMapped"])
        
        # Użyj left_outer join, aby zachować wszystkie kraje i dodać mapowaną nazwę
        df = df.join(F.broadcast(country_name_mapping), on="CountryNameRaw", how="left_outer") \
               .withColumn("CountryName", F.coalesce(F.col("CountryNameMapped"), F.col("CountryNameRaw"))) \
               .drop("CountryNameMapped", "CountryNameRaw") # Usuń surowe kolumny po mapowaniu

        return df

    def _unify_and_deduplicate(self, df: DataFrame) -> DataFrame:
        """
        Wykonuje ostateczną unifikację i deduplikację danych krajów ze wszystkich źródeł.
        Rozwiązuje konflikty i tworzy spójny, finalny model.
        """
        # 1. Priorytetyzacja danych lub scalanie informacji
        # Jeśli WHO ma lepsze dane (np. kody ISO), możemy preferować dane z WHO.
        # Tworzymy kolumnę 'priority' i wybieramy wiersze o najwyższym priorytecie.
        df_with_priority = df.withColumn("priority", 
            F.when(F.col("SourceSystem") == DomainSource.WHO.name, 2) # Wyższy priorytet dla WHO
             .when(F.col("SourceSystem") == DomainSource.NOBEL_PRIZES.name, 1)
             .otherwise(0)
        )
        
        # Używamy Window Functions do wyboru najlepszego rekordu dla każdego CountryName
        from pyspark.sql.window import Window
        window_spec = Window.partitionBy("CountryName").orderBy(F.desc("priority"))
        
        deduplicated_df = df_with_priority.withColumn("row_num", F.row_number().over(window_spec)) \
                                          .filter(F.col("row_num") == 1) \
                                          .drop("row_num", "priority")

        # 2. Ostateczne wybranie i uporządkowanie kolumn
        # Upewnij się, że wszystkie oczekiwane kolumny są obecne i mają prawidłowe typy.
        # Utwórz brakujące kolumny z wartościami null, jeśli jakieś źródło ich nie dostarczyło.
        final_df = deduplicated_df.select(
            F.col("CountryName").cast(StringType()).alias("CountryName"),
            F.col("ISOCode2Raw").alias("ISOCode2").cast(StringType()), # ISOCode2Raw z _standardize_source_data
            F.col("SourceSystem").cast(StringType()).alias("SourceSystem") # Pamiętaj o zachowaniu audytu
            # Dodaj inne kolumny, jeśli Twoje readery je zwracają i są potrzebne w modelu Silver
        ).distinct() # Końcowa deduplikacja, na wypadek gdyby coś jeszcze się prześlizgnęło

        # Ensure final schema matches expectations
        final_df = final_df.select(*[c.name for c in self._get_final_schema()])
        
        return final_df

    def _get_final_schema(self) -> StructType:
        """Definiuje finalny schemat modelu Country w Silver."""
        return StructType([
            StructField("CountryName", StringType(), True),
            StructField("ISOCode2", StringType(), True),
            StructField("SourceSystem", StringType(), True) # Dla audytu, skąd dane pochodzą
        ])