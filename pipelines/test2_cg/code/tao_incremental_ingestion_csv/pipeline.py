from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tao_incremental_ingestion_csv.config.ConfigStore import *
from tao_incremental_ingestion_csv.functions import *
from prophecy.utils import *
from tao_incremental_ingestion_csv.graph import *

def pipeline(spark: SparkSession) -> None:
    set_start_time(spark)
    df_control_table_consolidated = control_table_consolidated(spark)
    df_control_table_consolidated = df_control_table_consolidated.cache()
    df_Full_Control_Table = Full_Control_Table(spark, df_control_table_consolidated)

    if Config.var_full_load_mode_sfic_tao == "1":
        Full_Load(Config.Full_Load).apply(spark, df_control_table_consolidated)

    if Config.var_full_load_mode_sfic_tao == "0":
        Full_Load_for_Inc(Config.Full_Load_for_Inc).apply(spark, df_Full_Control_Table)

    df_Inc_Control_Table = Inc_Control_Table(spark, df_control_table_consolidated)

    if Config.var_full_load_mode_sfic_tao == "0":
        Inc_Load_for_Inc(Config.Inc_Load_for_Inc).apply(spark, df_Inc_Control_Table)

    df_Keys_Control_Table = Keys_Control_Table(spark, df_control_table_consolidated)

    if Config.var_full_load_mode_sfic_tao == "0":
        Keys_Load_for_Inc(Config.Keys_Load_for_Inc).apply(spark, df_Keys_Control_Table)

    Critical_Files_Check(spark)

def main():
    spark = SparkSession.builder\
                .enableHiveSupport()\
                .appName("tao_poc_ingest_raw_into_copper_and_create_view")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/test2_cg")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/test2_cg", config = Config)(pipeline)

if __name__ == "__main__":
    main()
