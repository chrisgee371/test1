from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from tao_incremental_ingestion_csv.functions import *

def Read_Deduplicate_Write_Delta_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from datetime import datetime
    copper_log_table = f"{Config.var_catalog_name}.{Config.var_log_container}.copper_load_log"
    glob_filter = f"*{Config.file_type}"
    # Remove the '.' from '.csv'
    clean_file_type = Config.file_type.replace(".", "") # ".csv" -> "csv"
    # Single f-string (avoid multiple adjacent f-strings that parser may flag)
    # If parser still errors, collapse to:
    raw_file_directory_path = f"abfss://{Config.var_raw_container}@{Config.var_raw_storage_account}.dfs.core.windows.net{Config.raw_file_path}"

    if Config.var_matching_files:
        csv_opts = {
            "header": "true",
            "multiLine": "true",
            "sep": f"{Config.field_separator}",
            "pathGlobFilter": glob_filter,
            "escape": '"'
        }
        copper_dataframe = tao_safe_read(
            raw_file_directory_path,
            clean_file_type,
            description = f"Read raw files for {Config.bronze_table}",
            copper_table = Config.bronze_table,
            options = csv_opts,
            severity = Config.var_error_type,
            log_type = Config.var_log_type_generic
        )
    else:
        copper_dataframe = None

    print(copper_dataframe)
    date_obj = datetime.strptime(Config.var_extract_bus_dt_tao_sfic, '%Y%m%d').date()

    try:
        log_message = f"Add metadata columns for {Config.bronze_table}"
        #log_message = (
        #    f"[{Config.var_info_type}][{Config.var_log_type_generic}][{Config.bronze_table}]"
        #    f"Add metadata columns for {Config.bronze_table}"
        #)
        copper_dataframe = copper_dataframe\
                               .withColumn(Config.var_aux_insert_datetime, current_timestamp())\
                               .withColumn(Config.var_aux_updated_datetime, lit(None).cast(TimestampType()))\
                               .withColumn(Config.var_aux_extract_bus_date, lit(str(date_obj)).cast(StringType()))\
                               .withColumn(Config.var_aux_updated_extract_bus_date, lit(None).cast(StringType()))
        tao_custom_logger(
            Config.var_info_type,
            Config.var_file_check,
            log_message,
            Config.bronze_table,
            copper_log_table,
            None,
            None,
            None,
            None,
            None
        )
        # check if there are duplicates
        log_message = f"Deduplication using row_number for {Config.bronze_table}"
        #log_message = (
        #    f"[{Config.var_info_type}][{Config.var_log_type_generic}][{Config.bronze_table}]"
        #    f"Deduplication using row_number for {Config.bronze_table}"
        #)
        partition_columns_list = [col(c.strip()) for c in Config.business_keys.split(",")]

        if Config.tie_breaker_column is None:
            window_spec = Window.partitionBy(*partition_columns_list).orderBy(lit(1))
        else:
            window_spec = Window\
                              .partitionBy(*partition_columns_list)\
                              .orderBy(copper_dataframe[Config.tie_breaker_column].desc())

        copper_dataframe_ranked = copper_dataframe.withColumn("row_number", row_number().over(window_spec))

        #delta_dataframe = copper_dataframe_ranked.filter(copper_dataframe_ranked["row_number"] == 1).drop("row_number")
        #error_dataframe = copper_dataframe_ranked.filter(copper_dataframe_ranked["row_number"] > 1).drop("row_number")
        return copper_dataframe_ranked
    except Exception as e:
        #log_message = (
        #    f"[{Config.var_error_type}][{Config.var_log_type_generic}][{Config.bronze_table}]"
        #)
        log_message = "Error during transform->" + log_message + str(e)
        tao_custom_logger(
            Config.var_error_type,
            Config.var_file_check,
            log_message,
            Config.bronze_table,
            copper_log_table,
            None,
            None,
            None,
            None,
            None
        )
        raise 

    return copper_raw_dataframe
