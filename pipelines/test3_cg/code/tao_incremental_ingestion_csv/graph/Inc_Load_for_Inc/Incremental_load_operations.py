from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from tao_incremental_ingestion_csv.functions import *

def Incremental_load_operations(spark: SparkSession, in0: DataFrame):
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
            mergekeys = (mergekeys + "existing.`" + escaped_key + "` = " + "new.`" + escaped_key + "` and ")

        # remove the extra 'and '
        mergekeys = mergekeys[:- 4]
        delta_table = DeltaTable.forName(spark, copper_table)
        delta_table_df = delta_table.toDF()
        # create the set of columns for the insert part of the merge operation
        # this needs to be done since we won't be inserting into all columns of the existing delta table
        # thus we can't use the WhenNotMatchedInsertAll method
        insert_columns_set = ""

        # we exclude the last columns of the delta table since those are the aux columns manually created
        for df_col in delta_table_df.columns[:- 4]:
            escaped_col = df_col.replace('"', '\\"')
            insert_columns_set = (
                insert_columns_set
                + '"existing.`'
                + escaped_col
                + '`": "new.`'
                + escaped_col
                + '`",'
            )

        update_columns_set = insert_columns_set
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # When inserting  new records we need the current time for the aux_ins_dtm field and the var_extract_bus_date_sfic for the aux_extract_bus_date
        insert_columns_set = (
            insert_columns_set
            + '"existing.'
            + Config.var_aux_insert_datetime
            + '": "\''
            + f"{current_time}"
            + '\'", "existing.'
            + Config.var_aux_extract_bus_date
            + '": "\''
            + f"Config.var_extract_bus_dt_tao_sfic"
            + "'\""
        )
        insert_columns_set = "{" + f"{insert_columns_set}" + "}"
        # When updating we need the current time for the aux_upd_dtm field and the var_extract_bus_date_sfic for the aux_upd_extract_bus_date
        update_columns_set = (
            update_columns_set
            + '"existing.'
            + Config.var_aux_updated_datetime
            + '": "\''
            + f"{current_time}"
            + '\'", "existing.'
            + Config.var_aux_updated_extract_bus_date
            + '": "\''
            + f"Config.var_extract_bus_dt_tao_sfic"
            + "'\""
        )
        update_columns_set = "{" + f"{update_columns_set}" + "}"
        # the merge operation requires dictionary objects to be passed
        insert_dict = json.loads(insert_columns_set)
        update_dict = json.loads(update_columns_set)
        log_message = f"Merge into {Config.bronze_table}"
        #log_message = (
        #    f"[{Config.var_info_type}][{Config.var_log_type_generic}][{Config.bronze_table}]"
        #    f"{message}"
        #)
        copper_dataframe = in0.drop("row_number")
        merge_result = (delta_table\
                           .alias("existing")\
                           .merge(copper_dataframe.alias("new"), mergekeys)\
                           .whenMatchedUpdate(set = update_dict)\
                           .whenNotMatchedInsert(values = insert_dict)\
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
        log_message = f"Gather Operational Metrics for {Config.bronze_table} Merge: "
        #log_message = (
        #    f"[{Config.var_info_type}][{Config.var_log_type_generic}][{Config.bronze_table}]"
        #    f"{message}"
        #)
        history = delta_table.history()
        latest_operation = history.orderBy("timestamp", ascending = False).first()

        if latest_operation and latest_operation["operation"] == "MERGE":
            operation_metrics = latest_operation["operationMetrics"]
            num_inserted = int(operation_metrics.get("numTargetRowsInserted", 0))
            num_updated = int(operation_metrics.get("numTargetRowsUpdated", 0))
            log_message = f"Number of rows inserted: {num_inserted} and Number of rows updated: {num_updated}"
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
                num_inserted,
                num_updated,
                None,
                None,
                None
            )
        else:
            log_message = f"Couldn't retrieve metrics of the merge operation"
            tao_custom_logger(
                Config.var_error_type,
                Config.var_row_count,
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
