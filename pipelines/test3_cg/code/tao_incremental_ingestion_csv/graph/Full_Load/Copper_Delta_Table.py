from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from tao_incremental_ingestion_csv.functions import *

def Copper_Delta_Table(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("overwriteSchema", True)\
        .option(
          "path",
          f"abfss://{Config.var_copper_container}@{Config.var_delta_storage_account}.dfs.core.windows.net/{Config.bronze_table}"
        )\
        .mode("overwrite")\
        .saveAsTable(f"`{Config.var_catalog_name}`.`{Config.var_copper_schema}`.`{Config.bronze_table}`")
