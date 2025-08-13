import pandas as pd
import io
import datetime
# The Snowpark package is required for Python Worksheets.
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit, when, lead, lag, dateadd, row_number, coalesce, last_value
from snowflake.snowpark.window import Window
import datetime
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType, DecimalType, DateType

def main(session: snowpark.Session):
    """
    This function performs a bi-temporal merge using Snowflake Snowpark DataFrames.
    It combines a history DataFrame with a delta DataFrame, propagates changes,
    and correctly establishes valid-time and transaction-time to create a final,
    merged bi-temporal history.
    """
    # Run the function and print the final DataFrame
    merged_history = bi_temporal_merge_pandas()
    print("Final Merged Bi-Temporal DataFrame:")
    print(merged_history)
    df = session.create_dataframe(data=merged_history)
    return df

def bi_temporal_merge_pandas():
    """
    This function demonstrates a bi-temporal merge process using the Pandas library.
    It combines history and delta data, propagates changes, and reconstructs the
    valid-time and transaction-time axes to produce a complete, merged history.
    
    This implementation is a more readable and idiomatic Python alternative to
    the previous Snowpark version for data manipulation tasks.
    """

    # --- 1. Define Sample Data as TSV Strings ---
    # In a real-world scenario, you would load these from files or a database.
    # The TSV format is used here to mimic the input structure from the user's
    # uploaded files.
    history_tsv = """
ASSET_ID	DATA_PROVIDER	START_TMS	END_TMS	LAST_CHG_TMS	IS_CURRENT	TXN_START_TMS	TXN_END_TMS	IS_LATEST_TXN	DATA_PROVIDER_TYPE	ASSET_COUNTRY	ASSET_CURRENCY	ASSET_PRICE	ASSET_MATURITY_TMS
1	ENTERPRISE	2023-01-01 00:00:00.000	2023-02-01 23:59:59.000	2023-01-01 00:00:00.000	false	2023-01-01 00:00:00.000	2023-01-14 23:59:59.000	false	INTERNAL	USA	USD	100.50000	2030-01-01 00:00:00.000
1	ENTERPRISE	2023-01-01 00:00:00.000	2023-02-01 23:59:59.000	2023-01-15 00:00:00.000	false	2023-01-15 00:00:00.000	2024-01-11 23:59:59.000	false	INTERNAL	USA	USD	101.00000	2030-01-01 00:00:00.000
1	ENTERPRISE	2023-02-02 00:00:00.000	2024-01-11 23:59:59.000	2024-01-12 10:01:01.000	false	2024-01-12 10:01:01.000	2024-01-12 10:01:04.000	false	INTERNAL	USA	USD	100.50000	2031-05-01 00:00:00.000
1	ENTERPRISE	2024-01-12 00:00:00.000		2024-01-12 10:01:05.000	true	2024-01-12 10:01:05.000		true	INTERNAL	USA	USD	103.00000	2025-12-01 00:00:00.000
2	BLOOMBERG	2023-05-10 00:00:00.000	2023-08-31 23:59:59.000	2023-05-10 00:00:00.000	false	2023-05-10 00:00:00.000	2023-08-31 23:59:59.000	false	VENDOR	CAN	CAD	1500.00000	2028-06-15 00:00:00.000
"""

    # uploaded files.
    history_tsv_unused = """
ASSET_ID	DATA_PROVIDER	START_TMS	END_TMS	LAST_CHG_TMS	IS_CURRENT	TXN_START_TMS	TXN_END_TMS	IS_LATEST_TXN	DATA_PROVIDER_TYPE	ASSET_COUNTRY	ASSET_CURRENCY	ASSET_PRICE	ASSET_MATURITY_TMS
1	ENTERPRISE	2023-01-01 00:00:00.000	2023-02-01 23:59:59.000	2023-01-01 00:00:00.000	false	2023-01-01 00:00:00.000	2023-01-14 23:59:59.000	false	INTERNAL	USA	USD	100.50000	2030-01-01 00:00:00.000
1	ENTERPRISE	2023-01-01 00:00:00.000	2023-02-01 23:59:59.000	2023-01-15 00:00:00.000	false	2023-01-15 00:00:00.000	2024-01-11 23:59:59.000	false	INTERNAL	USA	USD	101.00000	2030-01-01 00:00:00.000
1	ENTERPRISE	2023-02-02 00:00:00.000	2024-01-11 23:59:59.000	2024-01-12 10:01:01.000	false	2024-01-12 10:01:01.000	2024-01-12 10:01:04.000	false	INTERNAL	USA	USD	100.50000	2031-05-01 00:00:00.000
1	ENTERPRISE	2024-01-12 00:00:00.000		2024-01-12 10:01:05.000	true	2024-01-12 10:01:05.000		true	INTERNAL	USA	USD	103.00000	2025-12-01 00:00:00.000
2	BLOOMBERG	2023-05-10 00:00:00.000	2023-08-31 23:59:59.000	2023-05-10 00:00:00.000	false	2023-05-10 00:00:00.000	2023-08-31 23:59:59.000	false	VENDOR	CAN	CAD	1500.00000	2028-06-15 00:00:00.000
2	BLOOMBERG	2023-09-01 00:00:00.000	2024-06-30 23:59:59.000	2023-09-01 00:00:00.000	false	2023-09-01 00:00:00.000	2024-06-30 23:59:59.000	false	VENDOR	CAN	CAD	1512.25000	2028-06-15 00:00:00.000
2	BLOOMBERG	2024-07-01 00:00:00.000		2024-07-01 00:00:00.000	true	2024-07-01 00:00:00.000		true	VENDOR	CAN	CAD	1515.00000	2028-06-15 00:00:00.000
3	ENTERPRISE	2022-01-01 00:00:00.000	2022-12-31 23:59:59.000	2022-01-01 00:00:00.000	false	2022-01-01 00:00:00.000	2022-12-31 23:59:59.000	false	INTERNAL	GBR	GBP	85.20000	2025-01-01 00:00:00.000
3	ENTERPRISE	2023-01-01 00:00:00.000	2023-06-30 23:59:59.000	2023-01-01 00:00:00.000	false	2023-01-01 00:00:00.000	2023-06-30 23:59:59.000	false	INTERNAL	GBR	GBP	90.00000	2025-01-01 00:00:00.000
3	ENTERPRISE	2023-07-01 00:00:00.000		2023-07-01 00:00:00.000	true	2023-07-01 00:00:00.000		true	INTERNAL	GBR	GBP	92.50000	2025-01-01 00:00:00.000
"""
    
    # Note: The delta data no longer includes 'DATA_PROVIDER_TYPE'
    delta_tsv = """
ASSET_ID	DATA_PROVIDER	START_TMS	LAST_CHG_TMS	ASSET_COUNTRY	ASSET_CURRENCY	ASSET_PRICE	ASSET_MATURITY_TMS
1	ENTERPRISE	2023-01-01 00:00:00.000	2024-01-15 00:00:00.000	USA	USD	102.00000	
1	ENTERPRISE	2024-01-12 00:00:00.000	2024-01-20 00:00:00.000	USA	USD	103.00000	
"""

    # Read the TSV data into Pandas DataFrames
    df_history = pd.read_csv(io.StringIO(history_tsv), sep='\t', parse_dates=[
        "START_TMS", "END_TMS", "LAST_CHG_TMS", "TXN_START_TMS", "TXN_END_TMS", "ASSET_MATURITY_TMS"
    ])
    df_delta = pd.read_csv(io.StringIO(delta_tsv), sep='\t', parse_dates=[
        "START_TMS", "LAST_CHG_TMS", "ASSET_MATURITY_TMS"
    ])

    # --- 2. Unify and Prepare DataFrames ---
    # We only need the key columns and the data columns from the history
    df_history_simple = df_history[['ASSET_ID', 'DATA_PROVIDER', 'START_TMS', 'LAST_CHG_TMS',
                                   'DATA_PROVIDER_TYPE', 'ASSET_COUNTRY', 'ASSET_CURRENCY',
                                   'ASSET_PRICE', 'ASSET_MATURITY_TMS']]

    # The delta data is concatenated with the history, but it lacks 'DATA_PROVIDER_TYPE'.
    # We'll fill this missing column by forward-filling later.
    df_unified = pd.concat([df_history_simple, df_delta], ignore_index=True, sort=False)

    # Sort by valid-time (START_TMS) then transaction-time (LAST_CHG_TMS) to establish chronology
    df_unified.sort_values(by=['ASSET_ID', 'DATA_PROVIDER', 'START_TMS', 'LAST_CHG_TMS'], inplace=True)

    # --- 3. Handle Deletion Sentinels & Propagate Values ---
    # Define a map for deletion sentinels by column name
    sentinel_map = {
        # 'DATA_PROVIDER_TYPE' is no longer a data column and has been removed from this map.
        "ASSET_COUNTRY": '$$DELETED$$',
        "ASSET_CURRENCY": '$$DELETED$$',
        "ASSET_PRICE": 1234567890.12345,
        "ASSET_MATURITY_TMS": datetime.datetime(1900, 1, 1)
    }
    
    # Create copies of the original data to preserve deletion sentinels for a moment
    df_propagated = df_unified.copy()
    
    # Identify and replace sentinel values with NaN for forward-fill logic
    for col_name, sentinel_value in sentinel_map.items():
        if col_name in df_propagated.columns:
            if df_propagated[col_name].dtype == 'object': # Handle string sentinels
                df_propagated.loc[df_propagated[col_name] == sentinel_value, col_name] = pd.NA
            else: # Handle numeric/datetime sentinels
                df_propagated.loc[df_propagated[col_name] == sentinel_value, col_name] = None
    
    # Forward-fill missing values within each asset's timeline
    # 'DATA_PROVIDER_TYPE' has been removed from the data columns list.
    data_cols = ['ASSET_COUNTRY', 'ASSET_CURRENCY', 'ASSET_PRICE', 'ASSET_MATURITY_TMS']
    df_propagated[data_cols] = df_propagated.groupby(['ASSET_ID', 'DATA_PROVIDER'])[data_cols].ffill()

    # Also forward-fill the 'DATA_PROVIDER_TYPE' since it's a key attribute.
    # This ensures that new delta records get the correct type from the history.
    df_propagated['DATA_PROVIDER_TYPE'] = df_propagated.groupby(['ASSET_ID', 'DATA_PROVIDER'])['DATA_PROVIDER_TYPE'].ffill()
    
    # --- 4. Filter for Actual Changes and New Versions ---
    # Drop duplicates where the data hasn't changed. We group by all data columns
    # and keep the first occurrence of a change.
    df_deduped = df_propagated.drop_duplicates(
        subset=['ASSET_ID', 'DATA_PROVIDER', 'START_TMS', 'DATA_PROVIDER_TYPE'] + data_cols,
        keep='first'
    )
    
    # --- 5. Reconstruct Bi-Temporal Axes ---
    # Create the TXN_END_TMS by looking at the next LAST_CHG_TMS
    df_deduped['TXN_END_TMS'] = df_deduped.groupby(['ASSET_ID', 'DATA_PROVIDER', 'START_TMS'])['LAST_CHG_TMS'].shift(-1)
    # The transaction end time is one second before the next transaction starts
    df_deduped['TXN_END_TMS'] = df_deduped['TXN_END_TMS'] - pd.to_timedelta(1, unit='s')
    
    # Create the END_TMS by looking at the next START_TMS
    df_deduped['END_TMS'] = df_deduped.groupby(['ASSET_ID', 'DATA_PROVIDER'])['START_TMS'].shift(-1)
    # The valid end time is one second before the next valid start time
    df_deduped['END_TMS'] = df_deduped['END_TMS'] - pd.to_timedelta(1, unit='s')
    
    # Re-calculate the IS_CURRENT and IS_LATEST_TXN flags
    df_deduped['IS_CURRENT'] = df_deduped['END_TMS'].isna()
    df_deduped['IS_LATEST_TXN'] = df_deduped['TXN_END_TMS'].isna()

    # --- 6. Final Clean-up and Ordering ---
    # Rename the column for clarity
    final_df = df_deduped.rename(columns={'LAST_CHG_TMS': 'TXN_START_TMS'}).sort_values(
        by=['ASSET_ID', 'DATA_PROVIDER', 'START_TMS', 'TXN_START_TMS']
    )
    
    # Order the columns for the final output
    final_df = final_df[[
        "ASSET_ID", "DATA_PROVIDER", "DATA_PROVIDER_TYPE", "START_TMS", "END_TMS",
        "TXN_START_TMS", "TXN_END_TMS", "IS_CURRENT", "IS_LATEST_TXN",
        "ASSET_COUNTRY", "ASSET_CURRENCY", "ASSET_PRICE", "ASSET_MATURITY_TMS"
    ]]
    
    # Now, we re-propagate the deletion sentinels to their correct values
    for col_name, sentinel_value in sentinel_map.items():
        if final_df[col_name].dtype == 'object':
            final_df.loc[final_df[col_name].isna(), col_name] = sentinel_value
        else:
            final_df.loc[final_df[col_name].isna(), col_name] = sentinel_value

    return final_df.reset_index(drop=True)
