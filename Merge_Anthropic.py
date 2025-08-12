# The Snowpark package is required for Python Worksheets.
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit, when, lead, lag, dateadd, row_number, coalesce, greatest
from snowflake.snowpark.functions import sum as s_sum
from snowflake.snowpark.functions import max as s_max
from snowflake.snowpark.window import Window
import datetime
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType, DecimalType, DateType

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
        StructField("ASSET_MATURITY_TMS", TimestampType(), nullable=True),
        # Bi-temporal transaction-time columns in the history sample
        StructField("TXN_START_TMS", TimestampType(), nullable=True),
        StructField("TXN_END_TMS", TimestampType(), nullable=True),
        StructField("IS_LATEST_TXN", BooleanType(), nullable=True)
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
        # Asset 1
        (1, 'ENTERPRISE', 'INTERNAL', datetime.datetime(2023, 1, 1), datetime.datetime(2023, 2, 1, 23, 59, 59), datetime.datetime(2023, 1, 1), False, 'USA', 'USD', 100.50, datetime.datetime(2030, 1, 1),
         datetime.datetime(2023, 1, 1), None, True),
        (1, 'ENTERPRISE', 'INTERNAL', datetime.datetime(2023, 2, 2), datetime.datetime(2024, 1, 11, 23, 59, 59), datetime.datetime(2024, 1, 12, 10, 1, 1), False, 'USA', 'USD', 100.50, datetime.datetime(2031, 5, 1),
         datetime.datetime(2024, 1, 12, 10, 1, 1), None, True),
        (1, 'ENTERPRISE', 'INTERNAL', datetime.datetime(2024, 1, 12), None, datetime.datetime(2024, 1, 12, 10, 1, 5), True, 'USA', 'USD', 103.00, datetime.datetime(2025, 12, 1),
         datetime.datetime(2024, 1, 12, 10, 1, 5), None, True),

        # Asset 2 - multiple versions
        (2, 'BLOOMBERG', 'VENDOR', datetime.datetime(2023, 5, 10), datetime.datetime(2023, 8, 31, 23, 59, 59), datetime.datetime(2023, 5, 10), False, 'CAN', 'CAD', 1500.00, datetime.datetime(2028, 6, 15),
         datetime.datetime(2023, 5, 10), None, True),
        (2, 'BLOOMBERG', 'VENDOR', datetime.datetime(2023, 9, 1), datetime.datetime(2024, 6, 30, 23, 59, 59), datetime.datetime(2023, 9, 1), False, 'CAN', 'CAD', 1512.25, datetime.datetime(2028, 6, 15),
         datetime.datetime(2023, 9, 1), None, True),
        (2, 'BLOOMBERG', 'VENDOR', datetime.datetime(2024, 7, 1), None, datetime.datetime(2024, 7, 1), True, 'CAN', 'CAD', 1515.00, datetime.datetime(2028, 6, 15),
         datetime.datetime(2024, 7, 1), None, True),

        # Asset 3 - multiple versions
        (3, 'ENTERPRISE', 'INTERNAL', datetime.datetime(2022, 1, 1), datetime.datetime(2022, 12, 31, 23, 59, 59), datetime.datetime(2022, 1, 1), False, 'GBR', 'GBP', 85.20, datetime.datetime(2025, 1, 1),
         datetime.datetime(2022, 1, 1), None, True),
        (3, 'ENTERPRISE', 'INTERNAL', datetime.datetime(2023, 1, 1), datetime.datetime(2023, 6, 30, 23, 59, 59), datetime.datetime(2023, 1, 1), False, 'GBR', 'GBP', 90.00, datetime.datetime(2025, 1, 1),
         datetime.datetime(2023, 1, 1), None, True),
        (3, 'ENTERPRISE', 'INTERNAL', datetime.datetime(2023, 7, 1), None, datetime.datetime(2023, 7, 1), True, 'GBR', 'GBP', 92.50, datetime.datetime(2025, 1, 1),
         datetime.datetime(2023, 7, 1), None, True),
    ]

    delta_data = [
        # Asset 1: Historical correction at 2023-01-01
        # Only price is provided; other data columns are None (no change)
        (1, 'ENTERPRISE', 'INTERNAL', datetime.datetime(2023, 1, 1), datetime.datetime(2023, 1, 15), None, None, 101.00, None),

        # Asset 2: Historical correction and future dated new version
        # First: only maturity date changes; others None
        (2, 'BLOOMBERG', 'VENDOR', datetime.datetime(2023, 5, 10), datetime.datetime(2023, 6, 1), None, None, None, datetime.datetime(2028, 6, 16)),
        # Second: only price changes for future version
        (2, 'BLOOMBERG', 'VENDOR', datetime.datetime(2025, 1, 1), datetime.datetime(2024, 12, 15), None, None, 1525.00, None),
        # Deletion examples for Asset 2
        # Delete currency at 2023-09-01 (set to NULL via '$$DELETED$$')
        (2, 'BLOOMBERG', 'VENDOR', datetime.datetime(2023, 9, 1), datetime.datetime(2023, 9, 15), None, '$$DELETED$$', None, None),
        # Delete country at 2024-07-01 (set to NULL via '$$DELETED$$')
        (2, 'BLOOMBERG', 'VENDOR', datetime.datetime(2024, 7, 1), datetime.datetime(2024, 7, 10), '$$DELETED$$', None, None, None),

        # Asset 3: Historical correction and future dated new version
        # First: only country changes
        (3, 'ENTERPRISE', 'INTERNAL', datetime.datetime(2022, 1, 1), datetime.datetime(2022, 2, 1), 'GBR', None, None, None),
        # Second: only currency changes in future
        (3, 'ENTERPRISE', 'INTERNAL', datetime.datetime(2026, 3, 1), datetime.datetime(2025, 12, 1), None, 'GBP', None, None),
        # Deletion examples for Asset 3
        # Delete price at 2023-07-01 (set to NULL via 1234567890.12345 -> rounded to 1234567890.1235)
        (3, 'ENTERPRISE', 'INTERNAL', datetime.datetime(2023, 7, 1), datetime.datetime(2023, 7, 2), None, None, 1234567890.12345, None),

        # Asset 4: Brand new asset
        (4, 'EXTEL', 'VENDOR', datetime.datetime(2023, 7, 20), datetime.datetime(2023, 7, 20), 'JPN', 'JPY', 25000.00, datetime.datetime(2035, 1, 1)),
        # Deletion example for Asset 1: delete maturity at 2024-01-12 (set to NULL via 0001-01-01)
        (1, 'ENTERPRISE', 'INTERNAL', datetime.datetime(2024, 1, 12), datetime.datetime(2024, 1, 20), None, None, None, datetime.datetime(1, 1, 1)),
    ]

    df_history = session.create_dataframe(history_data, schema=history_schema)
    df_delta = session.create_dataframe(delta_data, schema=delta_schema)

    # --- 3. Bi-temporal Merge Logic ---

    # Define the natural key for an asset
    key_cols = ["ASSET_ID", "DATA_PROVIDER"]
    
    # Add missing columns to the delta frame before enrichment/unioning.
    df_delta_prepared = df_delta.with_column("END_TMS", lit(None).cast(TimestampType())) \
                                .with_column("IS_CURRENT", lit(None).cast(BooleanType())) \
                                .with_column("TXN_START_TMS", lit(None).cast(TimestampType())) \
                                .with_column("TXN_END_TMS", lit(None).cast(TimestampType())) \
                                .with_column("IS_LATEST_TXN", lit(None).cast(BooleanType()))

    # Build propagation candidates: cascade updates to future rows that carried forward old values
    hist_at_t0 = (
        df_history
        .select(
            *key_cols, "START_TMS",
            col("ASSET_COUNTRY").alias("H0_ASSET_COUNTRY"),
            col("ASSET_CURRENCY").alias("H0_ASSET_CURRENCY"),
            col("ASSET_PRICE").alias("H0_ASSET_PRICE"),
            col("ASSET_MATURITY_TMS").alias("H0_ASSET_MATURITY_TMS")
        )
    )

    delta_for_join = df_delta_prepared.select(
        *key_cols, "START_TMS",
        col("DATA_PROVIDER_TYPE").alias("DELTA_DATA_PROVIDER_TYPE"),
        col("ASSET_COUNTRY").alias("DELTA_ASSET_COUNTRY"),
        col("ASSET_CURRENCY").alias("DELTA_ASSET_CURRENCY"),
        col("ASSET_PRICE").alias("DELTA_ASSET_PRICE"),
        col("ASSET_MATURITY_TMS").alias("DELTA_ASSET_MATURITY_TMS"),
        col("LAST_CHG_TMS").alias("DELTA_LAST_CHG_TMS")
    )

    # Separate existing assets (need enrichment) from new assets (complete data)
    existing_asset_deltas_raw = (
        df_delta_prepared.join(
            df_history.select("ASSET_ID", "DATA_PROVIDER").distinct(),
            ["ASSET_ID", "DATA_PROVIDER"],
            "inner"
        )
    )
    
    new_assets_raw = (
        df_delta_prepared.join(
            df_history.select("ASSET_ID", "DATA_PROVIDER").distinct(),
            ["ASSET_ID", "DATA_PROVIDER"],
            "left_anti"
        )
    )

    # Only enrich existing asset deltas (Asset 4 doesn't need this since it has complete data)
    # For enrichment: get the H0 baseline from same START_TMS if exists, otherwise skip enrichment
    j0_enrichment = existing_asset_deltas_raw.join(
        hist_at_t0, 
        key_cols + ["START_TMS"], 
        "left"
    )

    # For propagation: need ALL deltas paired with ALL historical baselines for the same asset
    j0_propagation = delta_for_join.join(
        hist_at_t0, 
        key_cols, 
        "inner"
    )

    # Build enriched delta rows filling unspecified (null) data columns from H0
    enriched_existing_deltas = (
        j0_enrichment
        .filter(col("H0_ASSET_COUNTRY").is_not_null() | col("H0_ASSET_CURRENCY").is_not_null() | 
                col("H0_ASSET_PRICE").is_not_null() | col("H0_ASSET_MATURITY_TMS").is_not_null())
        .select(
            col("ASSET_ID"),
            col("DATA_PROVIDER"),
            col("DATA_PROVIDER_TYPE"),
            col("START_TMS"),
            lit(None).cast(TimestampType()).alias("END_TMS"),
            col("LAST_CHG_TMS"),
            lit(None).cast(BooleanType()).alias("IS_CURRENT"),
            coalesce(col("ASSET_COUNTRY"), col("H0_ASSET_COUNTRY")).alias("ASSET_COUNTRY"),
            coalesce(col("ASSET_CURRENCY"), col("H0_ASSET_CURRENCY")).alias("ASSET_CURRENCY"),
            coalesce(col("ASSET_PRICE"), col("H0_ASSET_PRICE")).alias("ASSET_PRICE"),
            coalesce(col("ASSET_MATURITY_TMS"), col("H0_ASSET_MATURITY_TMS")).alias("ASSET_MATURITY_TMS")
        )
        .with_column("TXN_START_TMS", lit(None).cast(TimestampType()))
        .with_column("TXN_END_TMS", lit(None).cast(TimestampType()))
        .with_column("IS_LATEST_TXN", lit(None).cast(BooleanType()))
    )

    # Handle existing asset deltas that couldn't be enriched (new START_TMS with no matching history)
    unenriched_existing_deltas = (
        j0_enrichment
        .filter(col("H0_ASSET_COUNTRY").is_null() & col("H0_ASSET_CURRENCY").is_null() & 
                col("H0_ASSET_PRICE").is_null() & col("H0_ASSET_MATURITY_TMS").is_null())
        .select(
            col("ASSET_ID"),
            col("DATA_PROVIDER"),
            col("DATA_PROVIDER_TYPE"),
            col("START_TMS"),
            lit(None).cast(TimestampType()).alias("END_TMS"),
            col("LAST_CHG_TMS"),
            lit(None).cast(BooleanType()).alias("IS_CURRENT"),
            col("ASSET_COUNTRY"),
            col("ASSET_CURRENCY"),
            col("ASSET_PRICE"),
            col("ASSET_MATURITY_TMS")
        )
        .with_column("TXN_START_TMS", lit(None).cast(TimestampType()))
        .with_column("TXN_END_TMS", lit(None).cast(TimestampType()))
        .with_column("IS_LATEST_TXN", lit(None).cast(BooleanType()))
    )

    # New assets use their complete delta data as-is (no enrichment needed)
    enriched_new_assets = (
        new_assets_raw.select(
            col("ASSET_ID"),
            col("DATA_PROVIDER"),
            col("DATA_PROVIDER_TYPE"),
            col("START_TMS"),
            col("END_TMS"),
            col("LAST_CHG_TMS"),
            col("IS_CURRENT"),
            col("ASSET_COUNTRY"),
            col("ASSET_CURRENCY"),
            col("ASSET_PRICE"),
            col("ASSET_MATURITY_TMS"),
            col("TXN_START_TMS"),
            col("TXN_END_TMS"),
            col("IS_LATEST_TXN")
        )
    )

    # Combine enriched existing, unenriched existing, and new assets
    enriched_delta = (
        enriched_existing_deltas
        .union_by_name(unenriched_existing_deltas)
        .union_by_name(enriched_new_assets)
    )

    future_hist = (
        df_history
        .select(
            *key_cols,
            col("START_TMS").alias("FUTURE_START_TMS"),
            col("DATA_PROVIDER_TYPE").alias("FUTURE_DATA_PROVIDER_TYPE"),
            col("ASSET_COUNTRY").alias("FUTURE_ASSET_COUNTRY"),
            col("ASSET_CURRENCY").alias("FUTURE_ASSET_CURRENCY"),
            col("ASSET_PRICE").alias("FUTURE_ASSET_PRICE"),
            col("ASSET_MATURITY_TMS").alias("FUTURE_ASSET_MATURITY_TMS"),
            col("LAST_CHG_TMS").alias("FUTURE_LAST_CHG_TMS")
        )
    )

    j_all = j0_propagation.join(
        future_hist,
        (j0_propagation["ASSET_ID"] == future_hist["ASSET_ID"]) &
        (j0_propagation["DATA_PROVIDER"] == future_hist["DATA_PROVIDER"]) &
        (future_hist["FUTURE_START_TMS"] > j0_propagation["START_TMS"]),
        "inner"
    )

    # Disambiguate duplicate key columns by selecting the left (delta/H0) keys and all needed future columns
    j_all = j_all.select(
        j0_propagation["ASSET_ID"].alias("ASSET_ID"),
        j0_propagation["DATA_PROVIDER"].alias("DATA_PROVIDER"),
        j0_propagation["START_TMS"].alias("START_TMS"),
        col("H0_ASSET_COUNTRY"), col("H0_ASSET_CURRENCY"), col("H0_ASSET_PRICE"), col("H0_ASSET_MATURITY_TMS"),
        col("DELTA_ASSET_COUNTRY"), col("DELTA_ASSET_CURRENCY"), col("DELTA_ASSET_PRICE"), col("DELTA_ASSET_MATURITY_TMS"), col("DELTA_LAST_CHG_TMS"),
        future_hist["FUTURE_START_TMS"], future_hist["FUTURE_DATA_PROVIDER_TYPE"],
        future_hist["FUTURE_ASSET_COUNTRY"], future_hist["FUTURE_ASSET_CURRENCY"], future_hist["FUTURE_ASSET_PRICE"],
        future_hist["FUTURE_ASSET_MATURITY_TMS"], future_hist["FUTURE_LAST_CHG_TMS"]
    )

    # Cascade only through immediate consecutive rows until first break per column
    prefix_window = Window.partitionBy("ASSET_ID", "DATA_PROVIDER", "START_TMS").orderBy(col("FUTURE_START_TMS").asc())

    j_marked = (
        j_all
        .with_column("country_break_cum", s_sum(when(col("FUTURE_ASSET_COUNTRY") != col("H0_ASSET_COUNTRY"), lit(1)).otherwise(lit(0))).over(prefix_window))
        .with_column("currency_break_cum", s_sum(when(col("FUTURE_ASSET_CURRENCY") != col("H0_ASSET_CURRENCY"), lit(1)).otherwise(lit(0))).over(prefix_window))
        .with_column("price_break_cum", s_sum(when(col("FUTURE_ASSET_PRICE") != col("H0_ASSET_PRICE"), lit(1)).otherwise(lit(0))).over(prefix_window))
        .with_column("maturity_break_cum", s_sum(when(col("FUTURE_ASSET_MATURITY_TMS") != col("H0_ASSET_MATURITY_TMS"), lit(1)).otherwise(lit(0))).over(prefix_window))
    )

    prop_country = col("DELTA_ASSET_COUNTRY").is_not_null() & (col("country_break_cum") == lit(0)) & (col("FUTURE_ASSET_COUNTRY") == col("H0_ASSET_COUNTRY")) & ((col("DELTA_ASSET_COUNTRY") != col("H0_ASSET_COUNTRY")) | (col("DELTA_ASSET_COUNTRY") == lit("$$DELETED$$")))
    prop_currency = col("DELTA_ASSET_CURRENCY").is_not_null() & (col("currency_break_cum") == lit(0)) & (col("FUTURE_ASSET_CURRENCY") == col("H0_ASSET_CURRENCY")) & ((col("DELTA_ASSET_CURRENCY") != col("H0_ASSET_CURRENCY")) | (col("DELTA_ASSET_CURRENCY") == lit("$$DELETED$$")))
    prop_price = col("DELTA_ASSET_PRICE").is_not_null() & (col("price_break_cum") == lit(0)) & (col("FUTURE_ASSET_PRICE") == col("H0_ASSET_PRICE")) & ((col("DELTA_ASSET_PRICE") != col("H0_ASSET_PRICE")) | (col("DELTA_ASSET_PRICE") == lit(1234567890.12345)))
    prop_maturity = col("DELTA_ASSET_MATURITY_TMS").is_not_null() & (col("maturity_break_cum") == lit(0)) & (col("FUTURE_ASSET_MATURITY_TMS") == col("H0_ASSET_MATURITY_TMS")) & ((col("DELTA_ASSET_MATURITY_TMS") != col("H0_ASSET_MATURITY_TMS")) | (col("DELTA_ASSET_MATURITY_TMS") == lit(datetime.datetime(1, 1, 1))))

    propagation_needed = j_marked.filter(prop_country | prop_currency | prop_price | prop_maturity)

    # Collapse multiple propagated updates per future row into a single combined synthetic row
    future_key_cols = ["ASSET_ID", "DATA_PROVIDER", "FUTURE_START_TMS"]

    # Build key sets per column and union them to avoid missing rows that only have a single-column propagation
    keys_country = j_marked.filter(prop_country).select(*future_key_cols).distinct()
    keys_currency = j_marked.filter(prop_currency).select(*future_key_cols).distinct()
    keys_price = j_marked.filter(prop_price).select(*future_key_cols).distinct()
    keys_maturity = j_marked.filter(prop_maturity).select(*future_key_cols).distinct()
    future_keys = (
        keys_country
        .union_by_name(keys_currency)
        .union_by_name(keys_price)
        .union_by_name(keys_maturity)
        .distinct()
    )

    # Direct enriched deltas that happen exactly at those future START_TMS (to merge with propagation)
    direct_future_enriched = (
        enriched_delta.join(
            future_keys.with_column_renamed("FUTURE_START_TMS", "START_TMS"),
            ["ASSET_ID", "DATA_PROVIDER", "START_TMS"],
            "inner"
        )
        .select(
            col("ASSET_ID"), col("DATA_PROVIDER"), col("START_TMS").alias("FUTURE_START_TMS"),
            col("LAST_CHG_TMS").alias("DIRECT_LAST_CHG_TMS"),
            col("ASSET_COUNTRY").alias("DIRECT_ASSET_COUNTRY"),
            col("ASSET_CURRENCY").alias("DIRECT_ASSET_CURRENCY"),
            col("ASSET_PRICE").alias("DIRECT_ASSET_PRICE"),
            col("ASSET_MATURITY_TMS").alias("DIRECT_ASSET_MATURITY_TMS")
        )
    )

    base_future = (
        future_keys.join(
            future_hist,
            future_key_cols,
            "inner"
        )
    )

    overall_synth = (
        propagation_needed
        .select(
            col("ASSET_ID"), col("DATA_PROVIDER"), col("FUTURE_START_TMS"),
            greatest(col("DELTA_LAST_CHG_TMS"), col("FUTURE_LAST_CHG_TMS")).alias("CAND_TMS")
        )
        .groupBy(*future_key_cols)
        .agg(s_max(col("CAND_TMS")).alias("SYNTH_LAST_CHG_TMS"))
    )

    # Make synthetic timestamps at least as new as any direct enriched delta at the same future start
    overall_synth = (
        overall_synth
        .join(
            direct_future_enriched.select(*future_key_cols, col("DIRECT_LAST_CHG_TMS")),
            future_key_cols,
            "left"
        )
        .select(
            *future_key_cols,
            # Ensure synthetic is always later than any direct delta at same start by adding 1 second
            dateadd('second', lit(1), greatest(col("SYNTH_LAST_CHG_TMS"), col("DIRECT_LAST_CHG_TMS"))).alias("SYNTH_LAST_CHG_TMS")
        )
    )

    w_desc = Window.partitionBy(*future_key_cols).orderBy(col("DELTA_LAST_CHG_TMS").desc())

    upd_country = (
        j_marked.filter(prop_country)
        .with_column("rn_country", row_number().over(w_desc))
        .filter(col("rn_country") == lit(1))
        .select(col("ASSET_ID"), col("DATA_PROVIDER"), col("FUTURE_START_TMS"), col("DELTA_ASSET_COUNTRY").alias("NEW_ASSET_COUNTRY"))
    )
    upd_currency = (
        j_marked.filter(prop_currency)
        .with_column("rn_currency", row_number().over(w_desc))
        .filter(col("rn_currency") == lit(1))
        .select(col("ASSET_ID"), col("DATA_PROVIDER"), col("FUTURE_START_TMS"), col("DELTA_ASSET_CURRENCY").alias("NEW_ASSET_CURRENCY"))
    )
    upd_price = (
        j_marked.filter(prop_price)
        .with_column("rn_price", row_number().over(w_desc))
        .filter(col("rn_price") == lit(1))
        .select(col("ASSET_ID"), col("DATA_PROVIDER"), col("FUTURE_START_TMS"), col("DELTA_ASSET_PRICE").alias("NEW_ASSET_PRICE"))
    )
    upd_maturity = (
        j_marked.filter(prop_maturity)
        .with_column("rn_maturity", row_number().over(w_desc))
        .filter(col("rn_maturity") == lit(1))
        .select(col("ASSET_ID"), col("DATA_PROVIDER"), col("FUTURE_START_TMS"), col("DELTA_ASSET_MATURITY_TMS").alias("NEW_ASSET_MATURITY_TMS"))
    )

    synthetic_rows = (
        base_future
        .join(overall_synth, future_key_cols, "left")
        .join(upd_country, future_key_cols, "left")
        .join(upd_currency, future_key_cols, "left")
        .join(upd_price, future_key_cols, "left")
        .join(upd_maturity, future_key_cols, "left")
        .join(direct_future_enriched, future_key_cols, "left")
        .select(
            col("ASSET_ID"),
            col("DATA_PROVIDER"),
            col("FUTURE_DATA_PROVIDER_TYPE").alias("DATA_PROVIDER_TYPE"),
            col("FUTURE_START_TMS").alias("START_TMS"),
            lit(None).cast(TimestampType()).alias("END_TMS"),
            coalesce(col("SYNTH_LAST_CHG_TMS"), col("DIRECT_LAST_CHG_TMS"), col("FUTURE_LAST_CHG_TMS")).alias("LAST_CHG_TMS"),
            lit(None).cast(BooleanType()).alias("IS_CURRENT"),
            coalesce(col("DIRECT_ASSET_COUNTRY"), col("NEW_ASSET_COUNTRY"), col("FUTURE_ASSET_COUNTRY")).alias("ASSET_COUNTRY"),
            coalesce(col("DIRECT_ASSET_CURRENCY"), col("NEW_ASSET_CURRENCY"), col("FUTURE_ASSET_CURRENCY")).alias("ASSET_CURRENCY"),
            coalesce(col("DIRECT_ASSET_PRICE"), col("NEW_ASSET_PRICE"), col("FUTURE_ASSET_PRICE")).alias("ASSET_PRICE"),
            coalesce(col("DIRECT_ASSET_MATURITY_TMS"), col("NEW_ASSET_MATURITY_TMS"), col("FUTURE_ASSET_MATURITY_TMS")).alias("ASSET_MATURITY_TMS")
        )
        .with_column("TXN_START_TMS", lit(None).cast(TimestampType()))
        .with_column("TXN_END_TMS", lit(None).cast(TimestampType()))
        .with_column("IS_LATEST_TXN", lit(None).cast(BooleanType()))
    )

    # Exclude enriched deltas that were merged into synthetic for those future keys
    enriched_delta_remaining = (
        enriched_delta.join(
            future_keys.with_column_renamed("FUTURE_START_TMS", "START_TMS"),
            ["ASSET_ID", "DATA_PROVIDER", "START_TMS"],
            "left_anti"
        )
    )

    # Union: history + remaining enriched deltas (both existing and new assets) + propagated synthetic 
    unioned_df = (
        df_history.with_column("source", lit("history"))
        .union_by_name(enriched_delta_remaining.with_column("source", lit("delta")))
        .union_by_name(synthetic_rows.with_column("source", lit("delta")))
    )

    # --- Bi-temporal construction ---
    # Transaction-time within each START_TMS: keep all rows and version by LAST_CHG_TMS
    txn_window = (
        Window.partitionBy("ASSET_ID", "DATA_PROVIDER", "START_TMS")
              .orderBy(col("LAST_CHG_TMS").asc())
    )

    with_txn = (
        unioned_df
        .with_column("TXN_START_TMS", col("LAST_CHG_TMS"))
        .with_column("next_txn_start", lead("LAST_CHG_TMS").over(txn_window))
        .with_column(
            "TXN_END_TMS",
            when(col("next_txn_start").is_not_null(), dateadd('second', lit(-1), col("next_txn_start")))
            .otherwise(lit(None).cast(TimestampType()))
        )
        .with_column("IS_LATEST_TXN", col("TXN_END_TMS").is_null())
        # Add validation: ensure TXN_START_TMS <= TXN_END_TMS when both are not null
        .with_column("txn_valid", 
                     when(col("TXN_END_TMS").is_null(), lit(True))
                     .otherwise(col("TXN_START_TMS") <= col("TXN_END_TMS")))
    )

    # Valid-time across distinct START_TMS values per asset
    distinct_start = (
        with_txn
        # choose one representative per (asset, provider, start) to build the valid timeline
        .with_column(
            "rn_per_start",
            row_number().over(
                Window.partitionBy("ASSET_ID", "DATA_PROVIDER", "START_TMS")
                      .orderBy(col("LAST_CHG_TMS").desc())
            )
        )
        .filter(col("rn_per_start") == 1)
        .with_column(
            "next_distinct_start",
            lead("START_TMS").over(
                Window.partitionBy("ASSET_ID", "DATA_PROVIDER").orderBy(col("START_TMS").asc())
            )
        )
        .select("ASSET_ID", "DATA_PROVIDER", "START_TMS", "next_distinct_start")
    )

    # Join back the valid-time next start to all transaction versions for that START_TMS
    processed_df = (
        with_txn.join(distinct_start, ["ASSET_ID", "DATA_PROVIDER", "START_TMS"])
        # Filter out any invalid transaction intervals (should be rare with fixed timestamps)
        .filter(col("txn_valid") == lit(True))
    )

    # --- 4. Finalize the new history table ---
    # Construct the final columns based on the processed data.
    final_df = (
        processed_df
        .with_column(
            "END_TMS",
            when(col("next_distinct_start").is_not_null(), dateadd('second', lit(-1), col("next_distinct_start")))
            .otherwise(lit(None).cast(TimestampType()))
        )
        .with_column("IS_CURRENT", col("END_TMS").is_null())
        .select(
            "ASSET_ID", "DATA_PROVIDER", "DATA_PROVIDER_TYPE", "START_TMS", "END_TMS",
            "LAST_CHG_TMS", "IS_CURRENT", "ASSET_COUNTRY", "ASSET_CURRENCY",
            "ASSET_PRICE", "ASSET_MATURITY_TMS",
            # transaction-time columns
            "TXN_START_TMS", "TXN_END_TMS", "IS_LATEST_TXN"
        )
        # Final validation: ensure no null LAST_CHG_TMS or TXN_START_TMS
        .filter(col("LAST_CHG_TMS").is_not_null() & col("TXN_START_TMS").is_not_null())
    )

    # Replace deletion sentinels with NULLs dynamically for all data columns
    excluded_cols = {
        "ASSET_ID", "DATA_PROVIDER", "DATA_PROVIDER_TYPE", "START_TMS", "END_TMS",
        "LAST_CHG_TMS", "IS_CURRENT", "TXN_START_TMS", "TXN_END_TMS", "IS_LATEST_TXN"
    }
    for field in final_df.schema.fields:
        name = field.name
        if name in excluded_cols:
            continue
        dtype = field.datatype
        if isinstance(dtype, StringType):
            final_df = final_df.with_column(name, when(col(name) == lit("$$DELETED$$"), lit(None).cast(dtype)).otherwise(col(name)))
        elif isinstance(dtype, DecimalType):
            final_df = final_df.with_column(name, when(col(name) == lit(1234567890.12345).cast(dtype), lit(None).cast(dtype)).otherwise(col(name)))
        elif isinstance(dtype, (TimestampType, DateType)):
            final_df = final_df.with_column(name, when(col(name) == lit(datetime.datetime(1, 1, 1)).cast(dtype), lit(None).cast(dtype)).otherwise(col(name)))
    final_df = final_df.sort("ASSET_ID", "DATA_PROVIDER", "START_TMS", "TXN_START_TMS", "IS_LATEST_TXN")
    # Return the final DataFrame. In a Snowflake Worksheet, this will be displayed as a results table.
    return final_df
