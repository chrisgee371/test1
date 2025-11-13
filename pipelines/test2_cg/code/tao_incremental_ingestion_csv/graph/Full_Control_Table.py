from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from tao_incremental_ingestion_csv.config.ConfigStore import *
from tao_incremental_ingestion_csv.functions import *

def Full_Control_Table(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(((col("inc_load") == lit(0)) & (col("full_load") == lit(1))))
