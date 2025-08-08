# The Snowpark package is required for Python Worksheets.
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit, when, lead, lag, dateadd
from snowflake.snowpark.window import Window
import datetime
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType, DecimalType

def main(session: snowpark.Session):
    """
    This function demonstrates a full bi-temporal merge process in Snowpark.
    It creates a history and delta dataframe, then applies the delta's changes
    (corrections, new versions, new assets) to the history dataframe.
    """

    # --- 1. Define Schemas ---
    history_schema = StructType([
        StructField("ASSET_ID", IntegerType(), nullable=False),
        StructField("DATA_PROVIDER", StringType(), nullable=False),
        StructField("DATA_PROVIDER_TYPE", StringType(), nullable=True),
        StructField("START_TMS", TimestampType(), nullable=False),
        StructField("END_TMS", TimestampType(), nullable=True),
        StructField("LAST_CHG_TMS", TimestampType(), nullable=False),
        StructField("IS_CURRENT", BooleanType(), nullable=False),
        StructField("ASSET_COUNTRY", StringType(), nullable=True),
        StructField("ASSET_CURRENCY", StringType(), nullable=True),
        StructField("ASSET_PRICE", DecimalType(18, 4), nullable=True),
        StructField("ASSET_MATURITY_TMS", TimestampType(), nullable=True)
    ])

    delta_schema = StructType([
        StructField("ASSET_ID", IntegerType(), nullable=False),
        StructField("DATA_PROVIDER", StringType(), nullable=False),
        StructField("DATA_PROVIDER_TYPE", StringType(), nullable=True),
        StructField("START_TMS", TimestampType(), nullable=False),
        StructField("LAST_CHG_TMS", TimestampType(), nullable=False),
        StructField("ASSET_COUNTRY", StringType(), nullable=True),
        StructField("ASSET_CURRENCY", StringType(), nullable=True),
        StructField("ASSET_PRICE", DecimalType(18, 4), nullable=True),
        StructField("ASSET_MATURITY_TMS", TimestampType(), nullable=True)
    ])

    # --- 2. Define and Create Sample DataFrames ---
    history_data = [
        (1, 'ENTERPRISE', 'INTERNAL', datetime.datetime(2023, 1, 1), datetime.datetime(2023, 2, 1, 23, 59, 59), datetime.datetime(2023, 1, 1), False, 'USA', 'USD', 100.50, datetime.datetime(2030, 1, 1)),
        (1, 'ENTERPRISE', 'INTERNAL', datetime.datetime(2023, 2, 2), None, datetime.datetime(2023, 2, 2), True, 'USA', 'USD', 102.75, datetime.datetime(2030, 1, 1)),
        (2, 'BLOOMBERG', 'VENDOR', datetime.datetime(2023, 5, 10), None, datetime.datetime(2023, 5, 10), True, 'CAN', 'CAD', 1500.00, datetime.datetime(2028, 6, 15)),
        (3, 'ENTERPRISE', 'INTERNAL', datetime.datetime(2022, 1, 1), datetime.datetime(2022, 12, 31, 23, 59, 59), datetime.datetime(2022, 1, 1), False, 'GBR', 'GBP', 85.20, datetime.datetime(2025, 1, 1)),
    ]

    delta_data = [
        # SCENARIO 1: Correction for Asset 1
        (1, 'ENTERPRISE', 'INTERNAL', datetime.datetime(2023, 1, 1), datetime.datetime(2023, 1, 15), 'USA', 'USD', 101.00, datetime.datetime(2030, 1, 1)),
        # SCENARIO 2: New Version for Asset 2
        (2, 'BLOOMBERG', 'VENDOR', datetime.datetime(2023, 8, 1), datetime.datetime(2023, 8, 1), 'CAN', 'CAD', 1510.50, datetime.datetime(2028, 6, 15)),
        # SCENARIO 3: New Asset 4
        (4, 'EXTEL', 'VENDOR', datetime.datetime(2023, 7, 20), datetime.datetime(2023, 7, 20), 'JPN', 'JPY', 25000.00, datetime.datetime(2035, 1, 1)),
    ]

    df_history = session.create_dataframe(history_data, schema=history_schema)
    df_delta = session.create_dataframe(delta_data, schema=delta_schema)

    # --- 3. Bi-temporal Merge Logic ---

    # Define the natural key for an asset
    key_cols = ["ASSET_ID", "DATA_PROVIDER"]
    
    # Add missing columns to the delta frame before unioning.
    df_delta_prepared = df_delta.withColumn("END_TMS", lit(None).cast(TimestampType())) \
                                .withColumn("IS_CURRENT", lit(None).cast(BooleanType()))

    # Union the history and prepared delta frames.
    unioned_df = (
        df_history.withColumn("source", lit("history"))
        .unionByName(df_delta_prepared.withColumn("source", lit("delta")))
    )

    # Define a window to operate over the history of each asset.
    window_spec = Window.partitionBy(*key_cols).orderBy(col("START_TMS").asc(), col("source").desc())

    # Process the combined data to handle corrections and create a clean timeline
    processed_df = (
        unioned_df
        # Remove duplicate records that represent a correction.
        .withColumn("rn", snowpark.functions.row_number().over(window_spec))
        .filter(col("rn") == 1)
        # Use the lead function to find the START_TMS of the *next* record.
        .withColumn("next_start_tms", lead("START_TMS").over(window_spec))
    )

    # --- 4. Finalize the new history table ---
    # Construct the final columns based on the processed data.
    final_df = (
        processed_df
        .withColumn(
            "END_TMS",
            # Use dateadd to subtract one second, which is the correct Snowpark/SQL approach.
            when(col("next_start_tms").isNotNull(), dateadd('second', lit(-1), col("next_start_tms")))
            .otherwise(lit(None).cast(TimestampType()))
        )
        .withColumn(
            "IS_CURRENT",
            col("END_TMS").isNull()
        )
        # Select the final columns in the correct order, dropping temporary ones.
        .select(
            "ASSET_ID", "DATA_PROVIDER", "DATA_PROVIDER_TYPE", "START_TMS", "END_TMS",
            "LAST_CHG_TMS", "IS_CURRENT", "ASSET_COUNTRY", "ASSET_CURRENCY",
            "ASSET_PRICE", "ASSET_MATURITY_TMS"
        )
    )

    # Return the final DataFrame. In a Snowflake Worksheet, this will be displayed as a results table.
    return final_df
