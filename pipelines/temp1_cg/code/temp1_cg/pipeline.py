from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from temp1_cg.config.ConfigStore import *
from temp1_cg.functions import *
from prophecy.utils import *

def pipeline(spark: SparkSession) -> None:
    pass

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("temp1_cg").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/temp1_cg")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/temp1_cg", config = Config)(pipeline)

if __name__ == "__main__":
    main()
