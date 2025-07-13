import asyncio
from typing import List, Any, Type

from pyspark.sql import SparkSession
from azure_databricks.common.orchestrators.base_orchestrator import BaseETLOrchestrator 

# WAŻNE: Tutaj NIE importujemy TransformerFactory ani żadnych konkretnych transformatorów!
# Główny ETLOrchestrator nic o nich nie wie.

class ETLOrchestrator:
    def __init__(self, spark: SparkSession, dbutils_obj: Any, 
                 orchestrators_to_run: List[BaseETLOrchestrator], env: str = "dev"):
        self.spark = spark
        self.dbutils_obj = dbutils_obj
        self.env = env
        
        for orch_instance in orchestrators_to_run:
            if not isinstance(orch_instance, BaseETLOrchestrator):
                raise TypeError(f"Wszystkie obiekty w 'orchestrators_to_run' muszą być instancjami BaseETLOrchestrator. Znaleziono: {type(orch_instance)}")

        self.orchestrators_to_run = orchestrators_to_run
        print("Inicjowanie głównego ETLOrchestrator.")

    async def run(self):
        print("\n=== Rozpoczynanie GŁÓWNEGO procesu ETL (orkiestracja pod-orkiestratorów) ===")
        if not self.orchestrators_to_run:
            print("Brak pod-orkiestratorów do uruchomienia. Proces zakończony.")
            return

        tasks = []
        for orchestrator in self.orchestrators_to_run:
            print(f"Tworzę zadanie dla pod-orkiestratora: {orchestrator} (wywołuję run())...")
            tasks.append(orchestrator.run()) 

        results = await asyncio.gather(*tasks, return_exceptions=True)

        print("\n=== GŁÓWNY proces ETL zakończony. Podsumowanie: ===")
        for i, result in enumerate(results):
            orchestrator_name = self.orchestrators_to_run[i]
            if isinstance(result, Exception):
                print(f" - {orchestrator_name}: BŁĄD: {result}")
            else:
                print(f" - {orchestrator_name}: Zakończono sukcesem.")