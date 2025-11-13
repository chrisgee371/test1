from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from tao_incremental_ingestion_csv.functions import *

def Drop_col_log_Copper_Load_Full_INC(spark: SparkSession, in0: DataFrame) -> DataFrame:
    copper_log_table = f"{Config.var_catalog_name}.{Config.var_log_container}.copper_load_log"
    log_message = f"Rows inserted in {Config.bronze_table}: {in0.count()} "
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
    copper_delta_table = in0.drop("row_number")

    return copper_delta_table

    return Copper_Delta_Table
