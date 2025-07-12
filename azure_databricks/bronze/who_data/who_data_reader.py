from azure_databricks.common.data_lake_reader import DataLakeReader

class WhoDataReader(DataLakeReader):
    def __init__(self, spark, target_container_path):
        super().__init__(spark, target_container_path)
    

    def read(self):
        return super().read()