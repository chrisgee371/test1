from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from tao_incremental_ingestion_csv.config.ConfigStore import *
from tao_incremental_ingestion_csv.functions import *

def control_table_consolidated(spark: SparkSession) -> DataFrame:
    return spark.read.table(
        f"`{Config.var_catalog_name}`.`{Config.var_etl_schema}`.`tao_sfic_control_table_consolidated`"
    )
