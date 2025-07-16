from src.azure_databricks.common.structures.structure_builder import StructureBuilder


class LocalStrucrtureBuilder(StructureBuilder):
    def __init__(self, spark, dbutils_obj, config):
        super().__init__(spark, dbutils_obj, config)

    def get_external_location_name(self, layer):
        pass

    def get_full_table_location(self, layer, base_table_name):
        pass


    def initialize_databricks_environment(self):
        pass