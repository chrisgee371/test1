from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from tao_incremental_ingestion_csv.functions import *

def Inc_Directory_Listing(spark: SparkSession) -> DataFrame:
    from prophecy.libs.utils import directory_listing

    return directory_listing(
        spark,
        f"abfss://{Config.var_raw_container}@{Config.var_raw_storage_account}.dfs.core.windows.net{Config.raw_file_path}",
        True,
        "*.*"
    )
