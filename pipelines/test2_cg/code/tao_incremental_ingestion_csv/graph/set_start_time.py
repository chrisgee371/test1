from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from tao_incremental_ingestion_csv.config.ConfigStore import *
from tao_incremental_ingestion_csv.functions import *

def set_start_time(spark: SparkSession):
    import json
    import sys
    # Set the timestamp as a configuration variable
    Config.var_start_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Define default values for all parameters
    default_parameters = {
        "var_wkf_name": "executed from Prophecy",
        "var_wkf_run_id": "executed from Prophecy-RunID",
        "var_bus_dt": "20250507",
        "var_bus_dt_dt": "2025-05-07",
        "var_aux_min_dt": "1900-01-01",
        "var_aux_max_dt": "9999-12-31",
        "var_unspecified": "Unspecified",
        "var_active": "Active",
        "var_inactive": "Inactive",
        "var_d_day_max_dt": "2200-12-31",
        "var_src_sf": "1",
        "var_src_hext": "2",
        "var_src_hfin": "3",
        "var_history_start_dt": "2018-08-01",
        "var_full_load_flg": "0",
        "var_inc_days_history": "7",
        "var_catalog_name": "chris_demos",
        "var_copper_schema": "demos",
        "var_bronze_schema": "demos",
        "var_silver_schema": "demos",
        "var_gold_schema": "demos",
        "var_etl_schema": "demos",
        "var_log_schema": "demos",
        "var_bus_dt_dt_end_of_month": "DUMMY",
        "var_snapshot_init_load_bf": "DUMMY",
        "var_snapshot_init_load_state": "DUMMY"
    }

    # Safely evaluate command-line arguments
    try:
        # Check if the -O flag is provided
        if "-O" in sys.argv:
            # Get the index of the -O flag and fetch the corresponding JSON string
            argument_index = sys.argv.index("-O") + 1

            if argument_index < len(sys.argv):
                argument = sys.argv[argument_index].strip()

                if argument:
                    # Parse the JSON string
                    parameters = json.loads(argument)
                    print(f"Parsed parameters: {parameters}")
                else:
                    # Empty argument provided
                    print("Empty argument detected after -O. Using default parameters.")
                    parameters = default_parameters
            else:
                # No argument provided after -O
                print("No argument provided after -O. Using default parameters.")
                parameters = default_parameters
        else:
            # No -O flag provided (likely running directly in Prophecy UI)
            print("No -O flag detected. Using default parameters.")
            parameters = default_parameters
    except json.JSONDecodeError as e:
        # JSON parsing error
        print(f"Error parsing JSON from arguments: {e}. Using default parameters.")
        parameters = default_parameters
    except Exception as e:
        # General error
        print(f"Error processing arguments: {e}. Using default parameters.")
        parameters = default_parameters

    # Assign parameters to Config attributes
    Config.var_bus_dt = parameters["var_bus_dt"]
    Config.var_bus_dt_dt = parameters["var_bus_dt_dt"]
    Config.var_aux_min_dt = parameters["var_aux_min_dt"]
    Config.var_aux_max_dt = parameters["var_aux_max_dt"]
    Config.var_unspecified = parameters["var_unspecified"]
    Config.var_active = parameters["var_active"]
    Config.var_inactive = parameters["var_inactive"]
    Config.var_d_day_max_dt = parameters["var_d_day_max_dt"]
    Config.var_src_sf = parameters["var_src_sf"]
    Config.var_src_hext = parameters["var_src_hext"]
    Config.var_src_hfin = parameters["var_src_hfin"]
    Config.var_history_start_dt = parameters["var_history_start_dt"]
    Config.var_full_load_flg = parameters["var_full_load_flg"]
    Config.var_inc_days_history = parameters["var_inc_days_history"]
    Config.var_catalog_name = parameters["var_catalog_name"]
    Config.var_copper_schema = parameters["var_copper_schema"]
    Config.var_bronze_schema = parameters["var_bronze_schema"]
    Config.var_silver_schema = parameters["var_silver_schema"]
    Config.var_gold_schema = parameters["var_gold_schema"]
    Config.var_etl_schema = parameters["var_etl_schema"]
    Config.var_log_schema = parameters["var_log_schema"]
    Config.var_wkf_run_id = parameters["var_wkf_run_id"]
    Config.var_wkf_name = parameters["var_wkf_name"]
    Config.var_bus_dt_dt_end_of_month = parameters["var_bus_dt_dt_end_of_month"]
    Config.var_snapshot_init_load_bf = parameters["var_snapshot_init_load_bf"]
    Config.var_snapshot_init_load_state = parameters["var_snapshot_init_load_state"]
    Config.var_task_name = "tao_incremental_ingestion_csv"
    Config.var_task_run_id = ""

    return 
