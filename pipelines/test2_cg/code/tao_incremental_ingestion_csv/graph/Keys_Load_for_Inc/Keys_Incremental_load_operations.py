from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from tao_incremental_ingestion_csv.functions import *

def Keys_Incremental_load_operations(spark: SparkSession, in0: DataFrame):
    from delta.tables import DeltaTable

    try:
        # split the business keys into a list
        copper_table = f"{Config.var_catalog_name}.{Config.var_copper_schema}.{Config.bronze_table}"
        copper_log_table = f"{Config.var_catalog_name}.{Config.var_log_schema}.copper_load_log"
        keys = Config.business_keys.split(",")
        # create the merge keys condition for the merge operation
        mergekeys = ""

        for key in keys:
            escaped_key = key.replace('"', '\\"')
            # one table will be aliased as 'existing', the other as 'new'
            mergekeys = (mergekeys + "existing.`" + escaped_key + "` = " + "keys.`" + escaped_key + "` and ")

        # remove the extra 'and '
        mergekeys = mergekeys[:- 4]
        delta_table = DeltaTable.forName(spark, copper_table)
        delta_table_df = delta_table.toDF()
        log_message = f"Delete rows from {Config.bronze_table}"
        #log_message = (
        #    f"[{Config.var_info_type}][{Config.var_log_type_generic}][{Config.bronze_table}]"
        #    f"{message}"
        #)
        keys_dataframe = in0.drop("row_number")
        merge_result = (delta_table\
                           .alias("existing")\
                           .merge(keys_dataframe.alias("keys"), mergekeys)\
                           .whenNotMatchedBySourceDelete()\
                           .execute())
        tao_custom_logger(
            Config.var_info_type,
            Config.var_log_type_generic,
            log_message,
            Config.bronze_table,
            copper_log_table,
            None,
            None,
            None,
            None,
            None
        )
        # Get the latest operation
        log_message = f"Gather Operational Metrics for {Config.bronze_table} delete: "
        #log_message = (
        #    f"[{Config.var_info_type}][{Config.var_log_type_generic}][{Config.bronze_table}]"
        #    f"{message}"
        #)
        history = delta_table.history()
        latest_operation = history.orderBy("timestamp", ascending = False).first()

        if latest_operation and latest_operation["operation"] == "MERGE":
            operation_metrics = latest_operation["operationMetrics"]
            num_deleted = int(operation_metrics.get("numTargetRowsDeleted", 0))
            log_message = f"Number of rows deleted: {num_deleted} "
            #log_message = (
            #    f"[{Config.var_info_type}][{Config.var_log_type_generic}][{Config.bronze_table}]"
            #    f"{message}"
            #)
            tao_custom_logger(
                Config.var_info_type,
                Config.var_row_count,
                log_message,
                Config.bronze_table,
                copper_log_table,
                None,
                None,
                num_deleted,
                None,
                None
            )
        else:
            log_message = f"Couldn't retrieve metrics of the DELETE operation"
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
    except Exception as e:
        #log_message = (
        #    f"[{Config.var_error_type}][{Config.var_log_type_generic}][{Config.bronze_table}]"
        #)
        log_message = "Error during transform->" + log_message + str(e)
        tao_custom_logger(
            Config.var_error_type,
            Config.var_log_type_generic,
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

    return 
