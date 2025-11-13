from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from tao_incremental_ingestion_csv.config.ConfigStore import *
from tao_incremental_ingestion_csv.functions import *

def Critical_Files_Check(spark: SparkSession):
    from pyspark.dbutils import DBUtils
    dbu = DBUtils(spark)
    copper_log_table = f"{Config.var_catalog_name}.{Config.var_log_container}.copper_load_log"
    tao_custom_logger(
        Config.var_info_type,
        Config.var_critical_file,
        f"Starting critical files check",
        None,
        copper_log_table,
        None,
        None,
        None,
        None,
        None
    )
    # Query sfic control table to retrieve rows where critical = 1
    result_df = spark.sql(
        f"SELECT bronze_table FROM {Config.var_catalog_name}.{Config.var_etl_schema}.tao_sfic_control_table_consolidated WHERE critical = 1"
    )
    # Columns to check
    columns_to_check = [Config.var_aux_extract_bus_date, Config.var_aux_updated_extract_bus_date]
    # Initialize a flag to track if all tables meet the condition
    all_tables_pass = True

    for row in result_df.collect():
        table_name = row["bronze_table"]
        # Query the table and count records where the columns match the extract bus dt for sfic
        # OR is not a performant operation but here should make no difference since we're just checking for the existence of 1 record
        query = f"SELECT COUNT(*) AS record_count FROM {Config.var_catalog_name}.{Config.var_copper_schema}.{table_name} WHERE {columns_to_check[0]} = {Config.var_extract_bus_dt_tao_sfic} OR {columns_to_check[1]} = {Config.var_extract_bus_dt_tao_sfic} LIMIT 1"

        try:
            count_df = spark.sql(query)
            record_count = count_df.collect()[0]["record_count"]

            if record_count == 0:
                tao_custom_logger(
                    Config.var_error_type,
                    Config.var_critical_file,
                    f"Table {table_name} failed the check. No records found for extract bus dt {Config.var_extract_bus_dt_tao_sfic}",
                    {table_name},
                    copper_log_table,
                    None,
                    None,
                    None,
                    None,
                    None
                )
                all_tables_pass = False
                break # Exit the loop early if a table fails the condition
            else:
                tao_custom_logger(
                    Config.var_info_type,
                    Config.var_critical_file,
                    f"Table {table_name} passed the check with at least one matching record for extract bus dt {Config.var_extract_bus_dt_tao_sfic}",
                    {table_name},
                    copper_log_table,
                    None,
                    None,
                    None,
                    None,
                    None
                )
        except Exception as e:
            tao_custom_logger(
                Config.var_error_type,
                Config.var_critical_file,
                f"Error querying table {table_name}: {str(e)}",
                {table_name},
                copper_log_table,
                None,
                None,
                None,
                None,
                None
            )
            all_tables_pass = False
            break

    if all_tables_pass:
        tao_custom_logger(
            Config.var_info_type,
            Config.var_critical_file,
            f"All tables passed the check",
            None,
            copper_log_table,
            None,
            None,
            None,
            None,
            None
        )
        dbu.jobs.taskValues.set(key = 'var_sfic_critical_pass', value = 1)
    else:
        tao_custom_logger(
            Config.var_error_type,
            Config.var_critical_file,
            f"One or more tables failed the check",
            None,
            copper_log_table,
            None,
            None,
            None,
            None,
            None
        )
        dbu.jobs.taskValues.set(key = 'var_sfic_critical_pass', value = 0)

    return 
