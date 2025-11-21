from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from tao_incremental_ingestion_csv.functions import *

def inc_Duplicate_Record_Logger(spark: SparkSession, in0: DataFrame):
    from pyspark.sql import functions as F
    from datetime import datetime
    error_table = f"{Config.var_catalog_name}.{Config.var_log_schema}.etl_error_log"
    var_timestp = f"{Config.var_start_timestamp}"

    if not in0.isEmpty():
        ts_val = datetime.strptime(var_timestp, "%Y-%m-%d %H:%M:%S")
        src_table_val = f"{Config.file_prefix}{Config.file_type}"
        json_bad_dataframe = in0.select(
            lit(ts_val).alias("ins_tms"),
            lit(f"{Config.var_bus_dt}").alias("bus_dt"),
            lit(src_table_val).alias("src_table"),
            lit(f"{Config.bronze_table}").alias("tgt_table"),
            lit("Write duplicated records for " f"{Config.bronze_table}").alias("error_message"),
            lit(f"{Config.var_copper_schema}").alias("error_stage"),
            to_json(struct("*"), options = {"ignoreNullFields" : "false"}).alias("data_payload"),
            lit(f"{Config.var_wkf_name}").alias("wkf_name"),
            lit(f"{Config.var_wkf_run_id}").alias("wkf_run_id"),
            lit(f"{Config.var_task_name}").alias("task_name"),
            lit(f"{Config.var_task_run_id}").alias("task_run_id")
        )\
            .write\
                                 .format("delta")\
                                 .mode("append")\
                                 .saveAsTable(error_table)
        copper_log_table = f"{Config.var_catalog_name}.{Config.var_log_container}.copper_load_log"
        log_message = f"Write duplicate records for {Config.bronze_table}: {in0.count()} "
        #log_message = (
        #    f"[{Config.var_info_type}][{Config.var_log_type_generic}][{Config.bronze_table}]"
        #    f"{message}"
        #)
        tao_custom_logger(
            Config.var_info_type,
            Config.var_row_count,
            log_message,
            #in0.count(),
            Config.bronze_table,
            copper_log_table,
            in0.count(),
            None,
            None,
            None,
            None
        )

    return 
