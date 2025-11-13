from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from tao_incremental_ingestion_csv.functions import *
from . import *
from .config import *


class Full_Load(MetaGemExec):

    def __init__(self, config):
        self.config = config
        super().__init__()

    def execute(self, spark: SparkSession, subgraph_config: SubgraphConfig) -> List[DataFrame]:
        Config.update(subgraph_config)
        df_Directory_Listing = Directory_Listing(spark)
        Defensive_checks_UDF(spark, df_Directory_Listing)
        df_Read_Deduplicate_Write_Delta = Read_Deduplicate_Write_Delta(spark)
        df_RowDistributor_1_out0, df_RowDistributor_1_out1 = RowDistributor_1(spark, df_Read_Deduplicate_Write_Delta)
        df_Drop_col_log_Copper_Load = Drop_col_log_Copper_Load(spark, df_RowDistributor_1_out0)
        Copper_Delta_Table(spark, df_Drop_col_log_Copper_Load)
        Duplicate_Record_Logger(spark, df_RowDistributor_1_out1)
        subgraph_config.update(Config)

    def apply(self, spark: SparkSession, in0: DataFrame, ) -> None:
        inDFs = []
        results = []
        conf_to_column = dict(
            [("bronze_table", "bronze_table"),  ("raw_file_path", "raw_file_path"),  ("file_prefix", "file_prefix"),              ("file_type", "file_type"),  ("encryption_type", "encryption_type"),              ("field_separator", "field_separator"),  ("part_file", "part_file"),  ("keys_file", "keys_file"),              ("headers_in_file", "headers_in_file"),  ("full_load", "full_load"),  ("inc_load", "inc_load"),              ("critical", "critical"),  ("business_keys", "business_keys"),              ("tie_breaker_column", "tie_breaker_column"),  ("enabled", "enabled"),  ("ins_dtm", "ins_dtm")]
        )

        if in0.count() > 1000:
            raise Exception(f"Config DataFrame row count::{in0.count()} exceeds max run count")

        for row in in0.collect():
            update_config = self.config.update_from_row_map(row, conf_to_column)
            _inputs = inDFs
            results.append(self.__run__(spark, update_config, *_inputs))

        return do_union(results)
