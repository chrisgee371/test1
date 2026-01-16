from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt, SString
from prophecy.cb.ui.uispec import *

from pyspark.sql import functions as F
from pyspark.sql.types import *
import re
from datetime import datetime, date

class Add_Unspecified_Rows(ComponentSpec):
    name: str = "Add_Unspecified_Rows"
    category: str = "Custom"

    def optimizeCode(self) -> bool:
        # Return whether code optimization is enabled for this component
        return True

    @dataclass(frozen=True)
    class Add_Unspecified_RowsProperties(ComponentProperties):
        # properties for the component with default values
        #catalog: SString = SString("hrdp_catalog_dev")
        #schema: SString = SString("gold")
        #table: SString = SString("d_diversity")
        default: SString = SString("default")

    def dialog(self) -> Dialog:
        # Define the UI dialog structure for the component
        return Dialog("Add_Unspecified_Rows").addElement(
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(PortSchemaTabs().importSchema(), "2fr")
        )

    def customOutputSchemaEnabled(self) -> bool:    
        return True

    def validate(self, context: WorkflowContext, component: Component[Add_Unspecified_RowsProperties]) -> List[Diagnostic]:
        # Validate the component's state
        return []

    def onChange(self, context: WorkflowContext, oldState: Component[Add_Unspecified_RowsProperties], newState: Component[Add_Unspecified_RowsProperties]) -> Component[
    Add_Unspecified_RowsProperties]:
        # Handle changes in the component's state and return the new state
        return newState

    class Add_Unspecified_RowsCode(ComponentCode):
        def __init__(self, newProps):
            self.props: Add_Unspecified_Rows.Add_Unspecified_RowsProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            import pyspark.sql.functions as F
            # Load the existing table
            trimmed_task_name = Config.var_task_name.replace("ppl_gold_", "")
            dim_table = Config.var_catalog_name + "." + Config.var_gold_schema + "." + trimmed_task_name
            seed_file = Config.var_catalog_name + "." + Config.var_bronze_schema + "." + "seed_unspecified_record_column_default_vw"

            dim_df = spark.table(dim_table)
            seed_df = spark.table(seed_file)

            # Extract the table name for filtering (e.g., 'd_business_framework_latest')
            table_name_only = dim_table.split('.')[-1]

            # Filter seed rows for the specific table or wildcard (%)
            seed_filtered = seed_df.filter(
                (F.col("table_name") == table_name_only) | (F.col("table_name") == "%")
            )

            # Extract column names and metadata from the dimension table
            dim_columns = [f.name for f in dim_df.schema.fields]
            nullable_map = {}
            for f in dim_df.schema.fields:
                if f.nullable:
                    nullable_map[f.name] = 'y'
                else:
                    nullable_map[f.name] = 'n'

            data_type_map = {}
            for f in dim_df.schema.fields:
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
                escaped = re.escape(pattern)
                regex_pattern = "^" + escaped.replace("___wildcard___", ".*") + "$"
                return re.match(regex_pattern, column_name) is not None

            # Collect seed rows for processing
            seed_rows = seed_filtered.collect()

            # Dictionary to store the unspecified values for each column
            unspecified_lookup = {}

            # Process each column in the dimension table
            for col in dim_columns:
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
                        row["data_type"] == col_data_type or
                        (col_data_type == "timestamp" and row["data_type"] == "date") or
                        row["data_type"] == "%"
                    ) and (
                        row["nullable"].lower() == col_nullable or row["nullable"].lower() == "%"
                    ):
                        filtered_rows.append(row)

                # Sort filtered_rows to prioritise specific matches
                filtered_rows.sort(
                    key=lambda row: (
                        row["column_name"] != "%",  # Prioritise specific column_name
                        row["data_type"] != "%",    # Prioritise specific data_type
                        row["nullable"].lower() != "%"  # Then prioritise specific nullable
                    ),
                    reverse=True  # Sort in descending order of specificity
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
            for field in dim_df.schema.fields:
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
                        print(f"Warning: Failed to cast value '{val}' for column '{col_name}' to {data_type}. Using None. Error: {e}")
                        cast_val = None
                unspecified_record[col_name] = cast_val

            unspecified_rows = spark.createDataFrame([unspecified_record], schema=dim_df.schema)

            target_columns = [f.name for f in dim_df.schema.fields]
            input_aligned = in0.select(*[c for c in target_columns if c in in0.columns])

            for col in target_columns:
                if col not in input_aligned.columns:
                    input_aligned = input_aligned.withColumn(col, F.lit(None))

            input_aligned = input_aligned.select(*target_columns)

            combined_df = input_aligned.unionByName(unspecified_rows, allowMissingColumns=True)
            return combined_df