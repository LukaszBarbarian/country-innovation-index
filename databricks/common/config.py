dbutils.widgets.text("bronze_path", "/mnt/bronze/raw_jsons/", "Bronze Raw JSON Path")
dbutils.widgets.text("silver_path", "/mnt/silver/processed_data/", "Silver Delta Table Path")

BRONZE_PATH_CONTAINER = dbutils.widgets.get("bronze_path")
SILVER_PATH_CONTAINER = dbutils.widgets.get("silver_path")