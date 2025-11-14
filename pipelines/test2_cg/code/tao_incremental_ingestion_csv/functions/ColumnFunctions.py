from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.lookups import (
    createLookup,
    createRangeLookup,
    lookup,
    lookup_last,
    lookup_match,
    lookup_count,
    lookup_row,
    lookup_row_reverse,
    lookup_nth
)
from .UDFs import *

def tao_custom_logger(
        severity: Column, 
        log_type: Column, 
        log_message: Column, 
        table_name: Column, 
        log_table_name: Column, 
        rows_inserted: Column, 
        rows_updated: Column, 
        rows_deleted: Column, 
        start_dtm: Column, 
        end_dtm: Column
):
    from datetime import datetime
    from pyspark.sql import SparkSession

    def log_copper_load(
            #spark: SparkSession,
            ins_tms,
            bus_dt,
            extract_bus_dt,
            src_sys,
            severity,
            log_type,
            log_message,
            wkf_name,
            wkf_run_id,
            task_name,
            task_run_id,
            copper_table = None,
            rows_inserted = None,
            rows_updated = None,
            rows_deleted = None,
            start_dtm = None,
            end_dtm = None
    ):
        spark = SparkSession.builder.getOrCreate()

        # Convert string timestamps to datetime if still strings
        def ensure_dt(v):

            if v is None:
                return None

            if isinstance(v, datetime):
                return v

            return datetime.strptime(v, "%Y-%m-%d %H:%M:%S")

        ins_tms = ensure_dt(ins_tms)
        start_dtm = ensure_dt(start_dtm)
        end_dtm = ensure_dt(end_dtm)

        def ensure_integer(v):

            if v is None:
                return None

            if isinstance(v, int):
                return v

            return v.cast(int)

        rows_inserted = ensure_integer(rows_inserted)
        rows_updated = ensure_integer(rows_updated)
        rows_deleted = ensure_integer(rows_deleted)
        row_vals = [(ins_tms, bus_dt, extract_bus_dt, src_sys, copper_table, severity, log_type, rows_inserted,
                     rows_updated, rows_deleted, start_dtm, end_dtm, log_message, wkf_name,
                     wkf_run_id, task_name, task_run_id)]
        schema = """
      ins_tms TIMESTAMP, bus_dt STRING, extract_bus_dt STRING, src_sys STRING,
      copper_table STRING, severity STRING, log_type STRING,
      rows_inserted INT, rows_updated INT, rows_deleted INT,
      start_dtm TIMESTAMP, end_dtm TIMESTAMP, log_message STRING,
      wkf_name STRING, wkf_run_id STRING, task_name STRING, task_run_id STRING
    """
        (spark.createDataFrame(row_vals, schema).write.format("delta").mode("append").saveAsTable(log_table_name))

    # Call
    #wkf_name=Config.var_wkf_name
    wkf_name = "executed from Prophecy"
    log_copper_load(
        #spark,
        ins_tms = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        bus_dt = "var_bus_dt",
        extract_bus_dt = "var_bus_dt",
        src_sys = "TAO_SFIC_G",
        severity = severity,
        log_type = log_type,
        log_message = log_message,
        wkf_name = "var_wkf_name",
        wkf_run_id = "var_wkf_run_id",
        task_name = "var_task_name",
        task_run_id = "var_task_run_id",
        copper_table = table_name,
        rows_inserted = rows_inserted,
        rows_updated = rows_updated,
        rows_deleted = rows_deleted,
        start_dtm = start_dtm,
        end_dtm = end_dtm
    )

def tao_safe_read(
        raw_file_directory_path: Column, 
        file_format: Column, 
        description: Column, 
        copper_table: Column, 
        options: Column, 
        severity: Column, 
        log_type: Column
):
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.types import StructType
    #copper_log_table = f"{Config.var_catalog_name}.{Config.var_log_container}.copper_load_log"
    copper_log_table = "chris_demos.demos.copper_load_log"

    def safe_read(
            #spark: SparkSession,
            path: str,
            *,
            fmt: str = "csv",
            description: str = "",
            copper_table: str | None = None,
            options: dict | None = None,
            schema: StructType | None = None,
            severity: str = "ERROR",
            log_type: str = "GENERIC",
            eval_rows: int = 1,
            on_error: str = "empty",  # "empty" | "none" | "raise"
            logger = None
    ) -> DataFrame | None:
        spark = SparkSession.builder.getOrCreate()
        """
    Safely read a dataset with minimal evaluation.
    """
        options = options or {}

        if logger is None:
            def logger(sev, ltype, msg, copper_table=None) -> str:
                print(f"[{sev}][{ltype}][{copper_table}] {msg}")

                return (f"[{sev}][{ltype}][{copper_table}] {msg}")

        try:
            reader = spark.read.format(fmt)

            for k, v in options.items():
                reader = reader.option(k, v)

            df = reader.schema(schema).load(path) if schema else reader.load(path)

            if eval_rows > 0:
                df.limit(eval_rows).count() # cheap validation

            log_message = logger("INFO", log_type, f"Success: {description}", copper_table)
            #log_message = logger(Config.var_info_type, log_type, f"Success: {description}", copper_table)
            #log_message = f"Read raw file for {copper_table} "
            log_message = description
            tao_custom_logger(
                #Config.var_info_type,
                "INFO",
                #Config.var_file_check,
                "FILE_CHECK",
                log_message,
                copper_table,
                copper_log_table,
                None,
                None,
                None,
                None,
                None
            )

            return df
        except Exception as e:
            #log_message = logger(severity, log_type, f"Error during read: {description}: {e}", copper_table)
            log_message = "Error during -> " + log_message + str(e)
            tao_custom_logger(
                #Config.var_error_type,
                "ERROR",
                #Config.var_file_check,
                "FILE_CHECK",
                log_message,
                copper_table,
                copper_log_table,
                None,
                None,
                None,
                None,
                None
            )

            if on_error == "raise":
                raise 

            if on_error == "empty":

                if schema:
                    return spark.createDataFrame([], schema)

                return spark.createDataFrame([], [])

            return None

    df = safe_read(
        #spark,
        raw_file_directory_path,
        fmt = file_format,
        description = description,
        copper_table = copper_table,
        options = options,
        severity = severity,
        log_type = log_type,
        on_error = "empty"  # or "none" or "raise"
    )

    return df
