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
    merged_history["ASSET_PRICE"] = pd.to_numeric(merged_history["ASSET_PRICE"], errors="coerce")
    merged_history["ASSET_MATURITY_TMS"] = pd.to_datetime(merged_history["ASSET_MATURITY_TMS"], errors="coerce")
    merged_history["START_TMS"] = pd.to_datetime(merged_history["START_TMS"], errors="coerce")
    merged_history["END_TMS"] = pd.to_datetime(merged_history["END_TMS"], errors="coerce")
    merged_history["TXN_START_TMS"] = pd.to_datetime(merged_history["TXN_START_TMS"], errors="coerce")
    merged_history["TXN_END_TMS"] = pd.to_datetime(merged_history["TXN_END_TMS"], errors="coerce")

    df = session.create_dataframe(data=merged_history)
    return df


def bi_temporal_merge_pandas():
    """
    Performs a bitemporal merge on sample data using pandas.
    """
    # --- 1) Sample data (you can replace with real loads) ---
    history_tsv = """
ASSET_ID	DATA_PROVIDER	START_TMS	END_TMS	LAST_CHG_TMS	IS_CURRENT	TXN_START_TMS	TXN_END_TMS	IS_LATEST_TXN	DATA_PROVIDER_TYPE	ASSET_COUNTRY	ASSET_CURRENCY	ASSET_PRICE	ASSET_MATURITY_TMS
1	ENTERPRISE	2023-01-01 00:00:00.000	2023-02-01 23:59:59.000	2023-01-01 00:00:00.000	false	2023-01-01 00:00:00.000	2023-01-14 23:59:59.000	false	INTERNAL	USA	USD	100.50000	2030-01-01 00:00:00.000
1	ENTERPRISE	2023-01-01 00:00:00.000	2023-02-01 23:59:59.000	2023-01-15 00:00:00.000	false	2023-01-15 00:00:00.000	2024-01-11 23:59:59.000	false	INTERNAL	USA	USD	101.00000	2030-01-01 00:00:00.000
1	ENTERPRISE	2023-02-02 00:00:00.000	2024-01-11 23:59:59.000	2024-01-12 10:01:01.000	false	2024-01-12 10:01:01.000	2024-01-12 10:01:04.000	false	INTERNAL	USA	USD	100.50000	2030-01-01 00:00:00.000
1	ENTERPRISE	2024-01-12 00:00:00.000	NaT	2024-01-12 10:01:05.000	true	2024-01-12 10:01:05.000	NaT	true	INTERNAL	USA	USD	103.00000	2025-12-01 00:00:00.000
2	BLOOMBERG	2023-05-10 00:00:00.000	2023-08-31 23:59:59.000	2023-05-10 00:00:00.000	false	2023-05-10 00:00:00.000	2023-08-31 23:59:59.000	false	VENDOR	CAN	CAD	1500.00000	2028-06-15 00:00:00.000
"""

    # Delta DOES NOT carry END_TMS; “NaN/NaT” in data cols means “unchanged”
    delta_tsv = """
ASSET_ID	DATA_PROVIDER	START_TMS	LAST_CHG_TMS	ASSET_COUNTRY	ASSET_CURRENCY	ASSET_PRICE	ASSET_MATURITY_TMS
1	ENTERPRISE	2023-01-01 00:00:00.000	2024-01-15 00:00:00.000	NaN	NaN	101.0	1000-01-01 00:00:00.000
1	ENTERPRISE	2025-01-12 00:00:00.000	2025-01-12 00:00:00.000	NaN	NaN	1234567890.12345	NaT
"""

    # Read inputs, being explicit about null values
    df_history = pd.read_csv(io.StringIO(history_tsv), sep="\t", parse_dates=[
        "START_TMS", "END_TMS", "LAST_CHG_TMS", "TXN_START_TMS", "TXN_END_TMS", "ASSET_MATURITY_TMS"
    ])
    df_delta = pd.read_csv(io.StringIO(delta_tsv), sep="\t", parse_dates=[
        "START_TMS", "LAST_CHG_TMS", "ASSET_MATURITY_TMS"
    ], na_values=['NaT', ''])

    # Build a lookup for historical END_TMS by (ASSET_ID, DATA_PROVIDER, START_TMS)
    hist_end_lookup = (
        df_history[["ASSET_ID", "DATA_PROVIDER", "START_TMS", "END_TMS"]]
        .drop_duplicates()
        .set_index(["ASSET_ID", "DATA_PROVIDER", "START_TMS"])["END_TMS"]
    )

    # --- 2) Historical update propagation ---
    extra_versions = []
    data_cols = ["ASSET_COUNTRY", "ASSET_CURRENCY", "ASSET_PRICE", "ASSET_MATURITY_TMS"]
    
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
                (df_history["END_TMS"].isna())
            ]
            if not open_hist.empty:
                closed_version = open_hist.iloc[0].copy()
                closed_version["END_TMS"] = start_tms - pd.to_timedelta(1, "s")
                closed_version["TXN_END_TMS"] = txn_start - pd.to_timedelta(1, "s")
                closed_version["IS_LATEST_TXN"] = False
                extra_versions.append(closed_version)
            continue
            
        hist_row = hist_match.iloc[-1]

        changed_cols = []
        for c in data_cols:
            is_changed = False
            if pd.notna(d[c]):
                if pd.isna(hist_row[c]) or d[c] != hist_row[c]:
                    is_changed = True
            if is_changed:
                changed_cols.append(c)

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
            cont_df = pd.DataFrame(contiguous)
            for _, r in cont_df.iterrows():
                new_version = r.copy()
                for c in changed_cols:
                    new_version[c] = d[c]
                new_version["LAST_CHG_TMS"] = txn_start
                new_version["END_TMS"] = r["END_TMS"]
                extra_versions.append(new_version)

    if extra_versions:
        df_delta = pd.concat([df_delta, pd.DataFrame(extra_versions)], ignore_index=True, sort=False)

    # --- 3) Attach the historical END_TMS to *all* delta rows ---
    if "END_TMS" not in df_delta.columns:
        df_delta["END_TMS"] = pd.NaT
    keys = list(zip(df_delta["ASSET_ID"], df_delta["DATA_PROVIDER"], df_delta["START_TMS"]))
    df_delta["END_TMS"] = [
        df_delta.loc[i, "END_TMS"] if pd.notna(df_delta.loc[i, "END_TMS"]) else hist_end_lookup.get(k, pd.NaT)
        for i, k in enumerate(keys)
    ]

    # --- 4) Unify history + delta ---
    keep_cols = [
        "ASSET_ID", "DATA_PROVIDER", "START_TMS", "END_TMS", "LAST_CHG_TMS",
        "DATA_PROVIDER_TYPE", "ASSET_COUNTRY", "ASSET_CURRENCY", "ASSET_PRICE", "ASSET_MATURITY_TMS"
    ]
    df_history_simple = df_history[keep_cols]
    df_unified = pd.concat([df_history_simple, df_delta[keep_cols]], ignore_index=True, sort=False)

    df_unified = df_unified.sort_values(by=["ASSET_ID", "DATA_PROVIDER", "START_TMS", "LAST_CHG_TMS"], kind="mergesort")
    
    # --- 5) Handle deletion sentinels and then forward-fill ---
    df_ff = df_unified.copy()
    
    sentinel_map = {
        "ASSET_COUNTRY": "$$DELETED$$",
        "ASSET_CURRENCY": "$$DELETED$$",
        "ASSET_PRICE": 1234567890.12345,
        "ASSET_MATURITY_TMS": datetime.datetime(1000, 1, 1),
    }
    for col_name, sentinel_value in sentinel_map.items():
        if col_name in df_ff.columns:
            if pd.api.types.is_datetime64_any_dtype(df_ff[col_name]):
                df_ff.loc[df_ff[col_name] == sentinel_value, col_name] = pd.NaT
            else:
                df_ff.loc[df_ff[col_name] == sentinel_value, col_name] = pd.NA

    # Corrected Forward Fill Logic: Group by START_TMS to prevent carry-over between different valid-time periods
    df_ff[data_cols] = df_ff.groupby(["ASSET_ID", "DATA_PROVIDER", "START_TMS"], group_keys=False)[data_cols].ffill()
    df_ff["DATA_PROVIDER_TYPE"] = df_ff.groupby(["ASSET_ID", "DATA_PROVIDER"], group_keys=False)["DATA_PROVIDER_TYPE"].ffill()

    # --- 6) Drop duplicates by *valid* state ---
    df_ff = df_ff.drop_duplicates(
        subset=[
            "ASSET_ID", "DATA_PROVIDER", "START_TMS", "END_TMS", "LAST_CHG_TMS",
            "DATA_PROVIDER_TYPE", "ASSET_COUNTRY", "ASSET_CURRENCY", "ASSET_PRICE", "ASSET_MATURITY_TMS"
        ],
        keep="first"
    )

    # --- 7) Rebuild transaction-time fields (TXN_START/END) per valid start ---
    df_ff = df_ff.sort_values(by=["ASSET_ID", "DATA_PROVIDER", "START_TMS", "LAST_CHG_TMS"], kind="mergesort")
    df_ff["TXN_START_TMS"] = df_ff["LAST_CHG_TMS"]
    
    # **FIXED** Corrected the column name from "TXN_END_ TMS" to "TXN_END_TMS"
    df_ff["TXN_END_TMS"] = df_ff.groupby(["ASSET_ID", "DATA_PROVIDER", "START_TMS"])["TXN_START_TMS"].shift(-1)
    df_ff["TXN_END_TMS"] = df_ff["TXN_END_TMS"] - pd.to_timedelta(1, unit="s")

    # --- 8) Derive bitemporal flags ---
    df_ff["IS_CURRENT"] = df_ff["END_TMS"].isna()
    df_ff["IS_LATEST_TXN"] = df_ff["TXN_END_TMS"].isna()

    # --- 9) Final column order ---
    final_df = df_ff[[
        "ASSET_ID", "DATA_PROVIDER", "DATA_PROVIDER_TYPE",
        "START_TMS", "END_TMS",    
        "TXN_START_TMS", "TXN_END_TMS",
        "IS_CURRENT", "IS_LATEST_TXN",
        "ASSET_COUNTRY", "ASSET_CURRENCY", "ASSET_PRICE", "ASSET_MATURITY_TMS"
    ]].reset_index(drop=True)

    return final_df
