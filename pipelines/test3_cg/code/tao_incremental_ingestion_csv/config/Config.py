from tao_incremental_ingestion_csv.graph.Full_Load.config.Config import SubgraphConfig as Full_Load_Config
from tao_incremental_ingestion_csv.graph.Full_Load_for_Inc.config.Config import (
    SubgraphConfig as Full_Load_for_Inc_Config
)
from tao_incremental_ingestion_csv.graph.Inc_Load_for_Inc.config.Config import SubgraphConfig as Inc_Load_for_Inc_Config
from tao_incremental_ingestion_csv.graph.Keys_Load_for_Inc.config.Config import (
    SubgraphConfig as Keys_Load_for_Inc_Config
)
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            var_raw_container: str=None,
            var_raw_storage_account: str=None,
            var_copper_container: str=None,
            var_delta_storage_account: str=None,
            var_catalog_name: str=None,
            var_etl_schema: str=None,
            var_log_schema: str=None,
            var_bus_dt: str=None,
            var_copper_schema: str=None,
            var_log_container: str=None,
            var_warning_type: str=None,
            var_info_type: str=None,
            var_error_type: str=None,
            var_file_check: str=None,
            var_critical_file: str=None,
            var_row_count: str=None,
            var_log_type_generic: str=None,
            var_bus_dt_dt: str=None,
            var_wkf_name: str=None,
            var_task_name: str=None,
            var_task_run_id: str=None,
            var_wkf_run_id: str=None,
            var_start_timestamp: str=None,
            var_src_tao_sfic: str=None,
            var_src_tao_snow: str=None,
            var_src_tao_8fold: str=None,
            var_matching_files: bool=None,
            var_aux_insert_datetime: str=None,
            var_aux_updated_datetime: str=None,
            var_aux_extract_bus_date: str=None,
            var_aux_updated_extract_bus_date: str=None,
            var_extract_bus_dt_tao_sfic: str=None,
            var_full_load_mode_sfic_tao: str=None,
            Full_Load: dict=None,
            Full_Load_for_Inc: dict=None,
            Inc_Load_for_Inc: dict=None,
            Keys_Load_for_Inc: dict=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            var_raw_container, 
            var_raw_storage_account, 
            var_copper_container, 
            var_delta_storage_account, 
            var_catalog_name, 
            var_etl_schema, 
            var_log_schema, 
            var_bus_dt, 
            var_copper_schema, 
            var_log_container, 
            var_warning_type, 
            var_info_type, 
            var_error_type, 
            var_file_check, 
            var_critical_file, 
            var_row_count, 
            var_log_type_generic, 
            var_bus_dt_dt, 
            var_wkf_name, 
            var_task_name, 
            var_task_run_id, 
            var_wkf_run_id, 
            var_start_timestamp, 
            var_src_tao_sfic, 
            var_src_tao_snow, 
            var_src_tao_8fold, 
            var_matching_files, 
            var_aux_insert_datetime, 
            var_aux_updated_datetime, 
            var_aux_extract_bus_date, 
            var_aux_updated_extract_bus_date, 
            var_extract_bus_dt_tao_sfic, 
            var_full_load_mode_sfic_tao, 
            Full_Load, 
            Full_Load_for_Inc, 
            Inc_Load_for_Inc, 
            Keys_Load_for_Inc
        )

    def update(
            self,
            var_raw_container: str="raw",
            var_raw_storage_account: str="hrdpintstgdev",
            var_copper_container: str="copper",
            var_delta_storage_account: str="hrdpdeltastgdev",
            var_catalog_name: str="hrdp_catalog_dev",
            var_etl_schema: str="etl",
            var_log_schema: str="log",
            var_bus_dt: str="20250929",
            var_copper_schema: str="copper",
            var_log_container: str="log",
            var_warning_type: str="WARNING",
            var_info_type: str="INFO",
            var_error_type: str="ERROR",
            var_file_check: str="FILE_CHECK",
            var_critical_file: str="CRITICAL_FILE_CHECK",
            var_row_count: str="ROW_COUNT",
            var_log_type_generic: str="GENERIC",
            var_bus_dt_dt: str="20250929",
            var_wkf_name: str="executed from Prophecy",
            var_task_name: str="tao_incremental_ingestion_csv",
            var_task_run_id: str="executed from Prophecy",
            var_wkf_run_id: str="executed from Prophecy",
            var_start_timestamp: str="1900-01-01 00:00:00",
            var_src_tao_sfic: str="TAO_SFIC_G",
            var_src_tao_snow: str="tao_snow",
            var_src_tao_8fold: str="tao_8fold",
            var_matching_files: bool=False,
            var_aux_insert_datetime: str="aux_ins_dtm",
            var_aux_updated_datetime: str="aux_upd_dtm",
            var_aux_extract_bus_date: str="aux_extract_bus_dt",
            var_aux_updated_extract_bus_date: str="aux_upd_extract_bus_dt",
            var_extract_bus_dt_tao_sfic: str="20251001",
            var_full_load_mode_sfic_tao: str="1",
            Full_Load: dict={},
            Full_Load_for_Inc: dict={},
            Inc_Load_for_Inc: dict={},
            Keys_Load_for_Inc: dict={},
            **kwargs
    ):
        prophecy_spark = self.spark
        self.var_raw_container = var_raw_container
        self.var_raw_storage_account = var_raw_storage_account
        self.var_copper_container = var_copper_container
        self.var_delta_storage_account = var_delta_storage_account
        self.var_catalog_name = var_catalog_name
        self.var_etl_schema = var_etl_schema
        self.var_log_schema = var_log_schema
        self.var_bus_dt = var_bus_dt
        self.var_copper_schema = var_copper_schema
        self.var_log_container = var_log_container
        self.var_warning_type = var_warning_type
        self.var_info_type = var_info_type
        self.var_error_type = var_error_type
        self.var_file_check = var_file_check
        self.var_critical_file = var_critical_file
        self.var_row_count = var_row_count
        self.var_log_type_generic = var_log_type_generic
        self.var_bus_dt_dt = var_bus_dt_dt
        self.var_wkf_name = var_wkf_name
        self.var_task_name = var_task_name
        self.var_task_run_id = var_task_run_id
        self.var_wkf_run_id = var_wkf_run_id
        self.var_start_timestamp = var_start_timestamp
        self.var_src_tao_sfic = var_src_tao_sfic
        self.var_src_tao_snow = var_src_tao_snow
        self.var_src_tao_8fold = var_src_tao_8fold
        self.var_matching_files = self.get_bool_value(var_matching_files)
        self.var_aux_insert_datetime = var_aux_insert_datetime
        self.var_aux_updated_datetime = var_aux_updated_datetime
        self.var_aux_extract_bus_date = var_aux_extract_bus_date
        self.var_aux_updated_extract_bus_date = var_aux_updated_extract_bus_date
        self.var_extract_bus_dt_tao_sfic = var_extract_bus_dt_tao_sfic
        self.var_full_load_mode_sfic_tao = var_full_load_mode_sfic_tao
        self.Full_Load = self.get_config_object(
            prophecy_spark, 
            Full_Load_Config(prophecy_spark = prophecy_spark), 
            Full_Load, 
            Full_Load_Config
        )
        self.Full_Load_for_Inc = self.get_config_object(
            prophecy_spark, 
            Full_Load_for_Inc_Config(prophecy_spark = prophecy_spark), 
            Full_Load_for_Inc, 
            Full_Load_for_Inc_Config
        )
        self.Inc_Load_for_Inc = self.get_config_object(
            prophecy_spark, 
            Inc_Load_for_Inc_Config(prophecy_spark = prophecy_spark), 
            Inc_Load_for_Inc, 
            Inc_Load_for_Inc_Config
        )
        self.Keys_Load_for_Inc = self.get_config_object(
            prophecy_spark, 
            Keys_Load_for_Inc_Config(prophecy_spark = prophecy_spark), 
            Keys_Load_for_Inc, 
            Keys_Load_for_Inc_Config
        )
        pass
