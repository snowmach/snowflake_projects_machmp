import pandas as pd
import io
import datetime
import snowflake.snowpark as snowpark

def main(session: snowpark.Session):
    """
    Main function to run the bitemporal merge logic and
    create a Snowpark DataFrame.
    """
    merged_history = bi_temporal_merge_pandas()

    # Print for worksheet debugging
    print("Final Merged Bi-Temporal DataFrame:")
    print(merged_history)

    # Enforce schema-friendly dtypes before sending to Snowflake
    for col in ["ASSET_PRICE"]:
        merged_history[col] = pd.to_numeric(merged_history[col], errors="coerce")
    for col in ["START_TMS", "END_TMS", "LAST_CHG_TMS", "TXN_START_TMS", "TXN_END_TMS", "ASSET_MATURITY_TMS"]:
        merged_history[col] = pd.to_datetime(merged_history[col], errors="coerce")

    df = session.create_dataframe(data=merged_history)
    return df


def bi_temporal_merge_pandas():
    """
    Performs a bitemporal merge on sample data using pandas.
    """
    # --- 1) Sample data ---
    history_tsv = """
ASSET_ID	DATA_PROVIDER	START_TMS	END_TMS	LAST_CHG_TMS	IS_CURRENT	TXN_START_TMS	TXN_END_TMS	IS_LATEST_TXN	DATA_PROVIDER_TYPE	ASSET_COUNTRY	ASSET_CURRENCY	ASSET_PRICE	ASSET_MATURITY_TMS
1	ENTERPRISE	2023-01-01 00:00:00.000	2023-02-01 23:59:59.000	2023-01-01 00:00:00.000	false	2023-01-01 00:00:00.000	2023-01-14 23:59:59.000	false	INTERNAL	USA	USD	100.50000	2030-01-01 00:00:00.000
1	ENTERPRISE	2023-01-01 00:00:00.000	2023-02-01 23:59:59.000	2023-01-15 00:00:00.000	false	2023-01-15 00:00:00.000	2024-01-11 23:59:59.000	false	INTERNAL	USA	USD	101.00000	2030-01-01 00:00:00.000
1	ENTERPRISE	2023-02-02 00:00:00.000	2024-01-11 23:59:59.000	2024-01-12 10:01:01.000	false	2024-01-12 10:01:01.000	2024-01-12 10:01:04.000	false	INTERNAL	USA	USD	100.50000	2030-01-01 00:00:00.000
1	ENTERPRISE	2024-01-12 00:00:00.000	NaT	2024-01-12 10:01:05.000	true	2024-01-12 10:01:05.000	NaT	true	INTERNAL	USA	USD	103.00000	2025-12-01 00:00:00.000
2	BLOOMBERG	2023-05-10 00:00:00.000	2023-08-31 23:59:59.000	2023-05-10 00:00:00.000	false	2023-05-10 00:00:00.000	2023-08-31 23:59:59.000	false	VENDOR	CAN	CAD	1500.00000	2028-06-15 00:00:00.000
"""

    delta_tsv = """
ASSET_ID	DATA_PROVIDER	START_TMS	LAST_CHG_TMS	ASSET_COUNTRY	ASSET_CURRENCY	ASSET_PRICE	ASSET_MATURITY_TMS
1	ENTERPRISE	2023-01-01 00:00:00.000	2024-01-15 00:00:00.000	NaN	NaN	101.0	1000-01-01 00:00:00.000
1	ENTERPRISE	2025-01-12 00:00:00.000	2025-01-12 00:00:00.000	NaN	NaN	1234567890.12345	NaT
1	ENTERPRISE	2025-05-12 00:00:00.000	2025-05-12 00:00:00.000	CAN	CAD	NaN	NaT
"""

    # Read inputs
    df_history = pd.read_csv(io.StringIO(history_tsv), sep="\t", parse_dates=[
        "START_TMS", "END_TMS", "LAST_CHG_TMS", "TXN_START_TMS", "TXN_END_TMS", "ASSET_MATURITY_TMS"
    ])
    df_delta = pd.read_csv(io.StringIO(delta_tsv), sep="\t", parse_dates=[
        "START_TMS", "LAST_CHG_TMS", "ASSET_MATURITY_TMS"
    ], na_values=['NaT', ''])

    # --- 2) Historical update propagation ---
    extra_versions = []
    data_cols = ["ASSET_COUNTRY", "ASSET_CURRENCY", "ASSET_PRICE", "ASSET_MATURITY_TMS"]
    
    df_delta = df_delta.sort_values("START_TMS")
    closed_open_records = set()

    for _, d in df_delta.iterrows():
        asset_id = d["ASSET_ID"]
        provider = d["DATA_PROVIDER"]
        start_tms = d["START_TMS"]
        txn_start = d["LAST_CHG_TMS"]

        hist_match = df_history[
            (df_history["ASSET_ID"] == asset_id) &
            (df_history["DATA_PROVIDER"] == provider) &
            (df_history["START_TMS"] == start_tms)
        ].sort_values("LAST_CHG_TMS")

        if hist_match.empty:
            open_hist = df_history[
                (df_history["ASSET_ID"] == asset_id) &
                (df_history["DATA_PROVIDER"] == provider) &
                (df_history["END_TMS"].isna()) &
                (df_history["START_TMS"] < start_tms)
            ].sort_values("LAST_CHG_TMS", ascending=False)
            
            if not open_hist.empty:
                open_record_key = (asset_id, provider, open_hist.iloc[0]["START_TMS"])
                if open_record_key not in closed_open_records:
                    closed_version = open_hist.iloc[0].copy()
                    closed_version["END_TMS"] = start_tms - pd.to_timedelta(1, "s")
                    closed_version["TXN_END_TMS"] = txn_start - pd.to_timedelta(1, "s")
                    closed_version["IS_CURRENT"] = False
                    closed_version["IS_LATEST_TXN"] = False
                    extra_versions.append(closed_version)
                    closed_open_records.add(open_record_key)
            continue
            
        hist_row = hist_match.iloc[-1]
        changed_cols = [c for c in data_cols if pd.notna(d[c]) and (pd.isna(hist_row[c]) or d[c] != hist_row[c])]

        if not changed_cols:
            continue

        future_rows = df_history[
            (df_history["ASSET_ID"] == asset_id) &
            (df_history["DATA_PROVIDER"] == provider) &
            (df_history["START_TMS"] > hist_row["START_TMS"])
        ].sort_values("START_TMS")

        contiguous = []
        for _, f in future_rows.iterrows():
            if any(pd.notna(hist_row[c]) and pd.notna(f[c]) and f[c] != hist_row[c] for c in changed_cols):
                break
            contiguous.append(f)

        if contiguous:
            for _, r in pd.DataFrame(contiguous).iterrows():
                new_version = r.copy()
                for c in changed_cols:
                    new_version[c] = d[c]
                new_version["LAST_CHG_TMS"] = txn_start
                extra_versions.append(new_version)

    if extra_versions:
        df_delta = pd.concat([df_delta, pd.DataFrame(extra_versions)], ignore_index=True, sort=False)

    # --- 3) Attach END_TMS to delta rows ---
    if "END_TMS" not in df_delta.columns:
        df_delta["END_TMS"] = pd.NaT
    keys = list(zip(df_delta["ASSET_ID"], df_delta["DATA_PROVIDER"], df_delta["START_TMS"]))
    df_delta["END_TMS"] = [
        df_delta.loc[i, "END_TMS"] if pd.notna(df_delta.loc[i, "END_TMS"]) 
        else df_history[
            (df_history["ASSET_ID"] == k[0]) & 
            (df_history["DATA_PROVIDER"] == k[1]) & 
            (df_history["START_TMS"] == k[2])
        ]["END_TMS"].iloc[0] if not df_history[
            (df_history["ASSET_ID"] == k[0]) & 
            (df_history["DATA_PROVIDER"] == k[1]) & 
            (df_history["START_TMS"] == k[2])
        ].empty else pd.NaT
        for i, k in enumerate(keys)
    ]

    # --- 4) Unify history + delta ---
    keep_cols = [
        "ASSET_ID", "DATA_PROVIDER", "START_TMS", "END_TMS", "LAST_CHG_TMS",
        "DATA_PROVIDER_TYPE", "ASSET_COUNTRY", "ASSET_CURRENCY", "ASSET_PRICE", "ASSET_MATURITY_TMS"
    ]
    df_unified = pd.concat([
        df_history[keep_cols],
        df_delta[keep_cols]
    ], ignore_index=True).sort_values(
        by=["ASSET_ID", "DATA_PROVIDER", "START_TMS", "LAST_CHG_TMS"], 
        kind="mergesort"
    )

    # --- 5) Handle deletions and forward-fill ---
    sentinel_map = {
        "ASSET_PRICE": 1234567890.12345,
        "ASSET_MATURITY_TMS": datetime.datetime(1000, 1, 1),
    }

    # First pass: Mark sentinel values as NA
    for col, sentinel in sentinel_map.items():
        if col in df_unified.columns:
            mask = df_unified[col] == sentinel
            if pd.api.types.is_datetime64_any_dtype(df_unified[col]):
                df_unified.loc[mask, col] = pd.NaT
            else:
                df_unified.loc[mask, col] = pd.NA

    # Second pass: For each deletion, find all records after it and set the column to NA
    for col, sentinel in sentinel_map.items():
        if col in df_unified.columns:
            # Find all deletion points (where sentinel was used)
            deletion_points = df_unified[df_unified[col] == sentinel][["ASSET_ID", "DATA_PROVIDER", "START_TMS"]]
            
            # For each deletion point, find all records in the same asset/provider with later START_TMS
            for _, deletion in deletion_points.iterrows():
                mask = (
                    (df_unified["ASSET_ID"] == deletion["ASSET_ID"]) &
                    (df_unified["DATA_PROVIDER"] == deletion["DATA_PROVIDER"]) &
                    (df_unified["START_TMS"] >= deletion["START_TMS"])
                )
                df_unified.loc[mask, col] = pd.NA

    # Now forward-fill the remaining NA values (that weren't part of deletions)
    group_cols = ["ASSET_ID", "DATA_PROVIDER"]
    for col in data_cols:
        if col in df_unified.columns:
            # Only fill NA values that aren't after a deletion
            fill_mask = df_unified[col].isna()
            if fill_mask.any():
                # Forward-fill within each asset/provider group
                df_unified.loc[fill_mask, col] = df_unified.groupby(
                    group_cols, group_keys=False
                )[col].ffill().loc[fill_mask]

    # Special handling for DATA_PROVIDER_TYPE which should always forward-fill
    df_unified["DATA_PROVIDER_TYPE"] = df_unified.groupby(
        group_cols, group_keys=False
    )["DATA_PROVIDER_TYPE"].ffill()

    # --- 6) Clean up duplicates ---
    df_clean = df_unified.drop_duplicates(
        subset=keep_cols,
        keep="first"
    ).sort_values(
        by=["ASSET_ID", "DATA_PROVIDER", "START_TMS", "LAST_CHG_TMS"],
        kind="mergesort"
    )

    # --- 7) Rebuild transaction time fields ---
    df_clean["TXN_START_TMS"] = df_clean["LAST_CHG_TMS"]
    df_clean["TXN_END_TMS"] = df_clean.groupby(
        ["ASSET_ID", "DATA_PROVIDER", "START_TMS"]
    )["TXN_START_TMS"].shift(-1)
    df_clean["TXN_END_TMS"] = df_clean["TXN_END_TMS"] - pd.to_timedelta(1, "s")

    # --- 8) Set END_TMS for records with future versions ---
    df_clean = df_clean.sort_values(["ASSET_ID", "DATA_PROVIDER", "START_TMS"])
    future_starts = df_clean.groupby(["ASSET_ID", "DATA_PROVIDER"])["START_TMS"].shift(-1)
    needs_end = df_clean["END_TMS"].isna() & future_starts.notna()
    df_clean.loc[needs_end, "END_TMS"] = future_starts[needs_end] - pd.to_timedelta(1, "s")

    # --- 9) Set bitemporal flags ---
    df_clean["IS_CURRENT"] = df_clean["END_TMS"].isna()
    df_clean["IS_LATEST_TXN"] = df_clean["TXN_END_TMS"].isna()

    # --- 10) Final output ---
    final_cols = [
        "ASSET_ID", "DATA_PROVIDER", "DATA_PROVIDER_TYPE",
        "START_TMS", "END_TMS", "LAST_CHG_TMS",
        "TXN_START_TMS", "TXN_END_TMS",
        "IS_CURRENT", "IS_LATEST_TXN",
        "ASSET_COUNTRY", "ASSET_CURRENCY", "ASSET_PRICE", "ASSET_MATURITY_TMS"
    ]
    final_df = df_clean[final_cols].reset_index(drop=True)
    
    return final_df
