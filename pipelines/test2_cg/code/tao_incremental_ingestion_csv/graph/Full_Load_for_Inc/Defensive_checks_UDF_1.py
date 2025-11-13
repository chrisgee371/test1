from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from tao_incremental_ingestion_csv.functions import *

def Defensive_checks_UDF_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F
    import fnmatch
    copper_log_table = f"{Config.var_catalog_name}.{Config.var_log_container}.copper_load_log"
    file_prefix = f"{Config.file_prefix}" + "*" + f"{Config.file_type}"
    return_df_row = []

    if in0.isEmpty():
        log_message = (
            f"The directory abfss://{Config.var_raw_container}@"
            f"{Config.var_raw_storage_account}{Config.raw_file_path} is empty"
        )
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
        Config.var_matching_files = False
    else:
        log_message = "Files found in the directory:\n"

        for row in in0.collect():
            size_in_mb = f"{row.size / (1024 * 1024):.2f} MB"
            log_message += (
                f"Path={row.path}, Name={row.name}, Size={size_in_mb}, "
                f"ModificationTime={row.modification_time}\n"
            )

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
        file_names = [row.name for row in in0.select("name").collect()]
        nr_files = len(file_names)
        matching_files = fnmatch.filter(file_names, file_prefix)
        nr_matching_files = len(matching_files)

        if matching_files:
            log_message = f"Files matching the pattern are {matching_files}. Number of matching files: {nr_matching_files}, out of a total of {nr_files} files"
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
            Config.var_matching_files = True
        else:
            log_message = f"No files matching the pattern were found. List of files found: {file_names}"
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
            Config.var_matching_files = False

    return_df_row = [(0)]
    return_df = spark.createDataFrame(return_df_row, ["return_df_row"])

    return return_df

    return out0
