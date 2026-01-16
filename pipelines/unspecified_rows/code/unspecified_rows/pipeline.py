from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from unspecified_rows.config.ConfigStore import *
from unspecified_rows.functions import *
from prophecy.utils import *
from unspecified_rows.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Add_Unspecified_Rows_0 = Add_Unspecified_Rows_0(spark)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("unspecified_rows").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/unspecified_rows")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/unspecified_rows", config = Config)(pipeline)

if __name__ == "__main__":
    main()
