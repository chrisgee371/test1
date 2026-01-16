from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from unspecified_rows.config.ConfigStore import *
from unspecified_rows.functions import *

def Copper_Delta_Table(spark: SparkSession) -> DataFrame:
    return spark.read.table("`chris_demos`.`demos`.`copper_delta_table`")
