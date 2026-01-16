from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from unspecified_rows.config.ConfigStore import *
from unspecified_rows.functions import *

def Add_Unspecified_Rows_0(spark: SparkSession, in0: DataFrame) -> DataFrame:
    import pyspark.sql.functions as F
    nullable_map = {}

    for f in spark.table(
        Config.var_catalog_name + "." + Config.var_gold_schema + "." + Config.var_task_name.replace("ppl_gold_", "")
    )\
        .schema\
        .fields:
        if f.nullable:
            nullable_map[f.name] = 'y'
        else:
            nullable_map[f.name] = 'n'

    data_type_map = {}

    for f in spark.table(
        Config.var_catalog_name + "." + Config.var_gold_schema + "." + Config.var_task_name.replace("ppl_gold_", "")
    )\
        .schema\
        .fields:
        if isinstance(f.dataType, StringType):
            data_type_map[f.name] = "string"
        elif isinstance(f.dataType, (IntegerType, LongType, FloatType, DoubleType)):
            data_type_map[f.name] = "number"
        elif isinstance(f.dataType, DateType):
            data_type_map[f.name] = "date"
        elif isinstance(f.dataType, BooleanType):
            data_type_map[f.name] = "boolean"
        elif isinstance(f.dataType, TimestampType):
            data_type_map[f.name] = "timestamp"
        else:
            data_type_map[f.name] = "other"

    # Function to match column names against patterns
    def column_matches_pattern(column_name: str, pattern: str) -> bool:
        pattern = pattern.replace("%", "___wildcard___")

        return re.match("^" + re.escape(pattern).replace("___wildcard___", ".*") + "$", column_name) is not None

    df1 = spark.table(
        (Config.var_catalog_name + "." + Config.var_bronze_schema + "." + "seed_unspecified_record_column_default_vw")
    )
    df7 = df1.filter(
        (
          (
            F.col("table_name")
            == (
              Config.var_catalog_name
              + "."
              + Config.var_gold_schema
              + "."
              + Config.var_task_name.replace("ppl_gold_", "")
            )\
              .split(
              '.'
            )[- 1]
          )
          | (F.col("table_name") == "%")
        )
    )
    # Collect seed rows for processing
    seed_rows = df7.collect()
    # Dictionary to store the unspecified values for each column
    unspecified_lookup = {}

    # Process each column in the dimension table
    for col in [
        f.name
        for f in spark.table(
          Config.var_catalog_name + "." + Config.var_gold_schema + "." + Config.var_task_name.replace("ppl_gold_", "")
        )\
          .schema\
          .fields
    ]:
        col_data_type = data_type_map[col]
        col_nullable = nullable_map[col]
        # Find matching rows for the column or wildcard (%)
        matching_rows = []

        for row in seed_rows:
            if row["column_name"] == col or row["column_name"] == "%":
                matching_rows.append(row)

        # Filter rows by data_type and nullable
        filtered_rows = []

        for row in matching_rows:
            if (
                (
                  row["data_type"] == col_data_type
                  or (
                    col_data_type == "timestamp"
                    and row["data_type"] == "date"
                  )
                  or row["data_type"] == "%"
                )
                and (
                  row["nullable"].lower() == col_nullable
                  or row["nullable"].lower() == "%"
                )
            ):
                filtered_rows.append(row)

        # Sort filtered_rows to prioritise specific matches
        filtered_rows.sort(
            key = lambda row: (                         row["column_name"] != "%",  # Prioritise specific column_name
                         row["data_type"] != "%",  # Prioritise specific data_type
                         row["nullable"].lower() != "%"),
            reverse = True  # Sort in descending order of specificity
        )

        # Assign the first match or fallback to global default
        if len(filtered_rows) > 0:
            value = filtered_rows[0]["unspecified_value"]
        else:
            value = None

        if value is not None:
            unspecified_lookup[col] = value

    # Create a dictionary to store the casted values for the unspecified record
    unspecified_record = {}

    # Cast the unspecified values to the appropriate data types
    for field in spark.table(
        Config.var_catalog_name + "." + Config.var_gold_schema + "." + Config.var_task_name.replace("ppl_gold_", "")
    )\
        .schema\
        .fields:
        col_name = field.name
        data_type = field.dataType
        val = unspecified_lookup.get(col_name)

        if val is None:
            cast_val = None
        else:
            try:
                if isinstance(data_type, StringType):
                    cast_val = str(val)
                elif isinstance(data_type, IntegerType):
                    cast_val = int(val)
                elif isinstance(data_type, LongType):
                    cast_val = int(val)
                elif isinstance(data_type, FloatType):
                    cast_val = float(val)
                elif isinstance(data_type, DoubleType):
                    cast_val = float(val)
                elif isinstance(data_type, BooleanType):
                    if isinstance(val, bool):
                        cast_val = val
                    else:
                        cast_val = val.lower() == 'true'
                elif isinstance(data_type, DateType):
                    if str(val).lower() == 'sysdate':
                        cast_val = date.today()
                    else:
                        try:
                            cast_val = datetime.strptime(val, "%Y-%m-%d").date()
                        except ValueError:
                            cast_val = datetime.strptime(val, "%Y%m%d").date()
                elif isinstance(data_type, TimestampType):
                    if str(val).lower() == 'sysdate':
                        cast_val = datetime.now()
                    elif len(str(val)) == 10:  # e.g., '1900-01-01'
                        try:
                            cast_val = datetime.strptime(val + " 00:00:00", "%Y-%m-%d %H:%M:%S")
                        except ValueError:
                            cast_val = datetime.strptime(val + " 00:00:00", "%Y%m%d %H:%M:%S")
                    elif len(str(val)) == 8:  # e.g., '19000101'
                        cast_val = datetime.strptime(val + " 00:00:00", "%Y%m%d %H:%M:%S")
                    else:
                        cast_val = datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
                else:
                    cast_val = str(val)
            except Exception as e:
                print(
                    f"Warning: Failed to cast value '{val}' for column '{col_name}' to {data_type}. Using None. Error: {e}"
                )
                cast_val = None

        unspecified_record[col_name] = cast_val

    df2 = spark.table(
        Config.var_catalog_name + "." + Config.var_gold_schema + "." + Config.var_task_name.replace("ppl_gold_", "")
    )
    df3 = in0.select(*[c for c in [f.name for f in df2.schema.fields] if c in in0.columns])
    input_aligned = df3

    for col in [
        f.name
        for f in spark.table(
          Config.var_catalog_name + "." + Config.var_gold_schema + "." + Config.var_task_name.replace("ppl_gold_", "")
        )\
          .schema\
          .fields
    ]:
        if col not in input_aligned.columns:
            input_aligned = input_aligned.withColumn(col, F.lit(None))

    df4 = spark.table(
        Config.var_catalog_name + "." + Config.var_gold_schema + "." + Config.var_task_name.replace("ppl_gold_", "")
    )
    input_aligned = input_aligned.select(*[f.name for f in df4.schema.fields])
    df5 = spark.table(
        Config.var_catalog_name + "." + Config.var_gold_schema + "." + Config.var_task_name.replace("ppl_gold_", "")
    )
    df6 = spark.createDataFrame([unspecified_record], schema = df5.schema)

    return input_aligned.unionByName(df6, allowMissingColumns = True)
