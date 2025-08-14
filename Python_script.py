import pandas as pd
import io
import datetime
import snowflake.snowpark as snowpark

def main(session: snowpark.Session):
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
1	ENTERPRISE	2023-01-01 00:00:00.000	2024-01-15 00:00:00.000	NaN	NaN	NaN	2040-01-01 00:00:00.000
1	ENTERPRISE	2025-01-12 00:00:00.000	2025-01-12 00:00:00.000	NaN	NaN	105.00000	NaT
"""

    # Read inputs
    df_history = pd.read_csv(io.StringIO(history_tsv), sep="\t", parse_dates=[
        "START_TMS", "END_TMS", "LAST_CHG_TMS", "TXN_START_TMS", "TXN_END_TMS", "ASSET_MATURITY_TMS"
    ])
    df_delta = pd.read_csv(io.StringIO(delta_tsv), sep="\t", parse_dates=[
        "START_TMS", "LAST_CHG_TMS", "ASSET_MATURITY_TMS"
    ])

    # Build a lookup for historical END_TMS by (ASSET_ID, DATA_PROVIDER, START_TMS)
    hist_end_lookup = (
        df_history[["ASSET_ID", "DATA_PROVIDER", "START_TMS", "END_TMS"]]
        .drop_duplicates()
        .set_index(["ASSET_ID", "DATA_PROVIDER", "START_TMS"])["END_TMS"]
    )

    # --- 2) Historical update propagation ---
    # If Delta updates a past start, propagate to future contiguous rows that shared the
    # same value(s) in the changed column(s) at that time, creating a new txn version for each.
    extra_versions = []
    data_cols = ["ASSET_COUNTRY", "ASSET_CURRENCY", "ASSET_PRICE", "ASSET_MATURITY_TMS"]

    for _, d in df_delta.iterrows():
        asset_id = d["ASSET_ID"]
        provider = d["DATA_PROVIDER"]
        start_tms = d["START_TMS"]
        txn_start = d["LAST_CHG_TMS"]

        # Find matching historical row (same valid start)
        hist_match = df_history[
            (df_history["ASSET_ID"] == asset_id) &
            (df_history["DATA_PROVIDER"] == provider) &
            (df_history["START_TMS"] == start_tms)
        ].sort_values("LAST_CHG_TMS")

        if hist_match.empty:
            # *** MODIFIED LOGIC START ***
            # If Delta introduces a brand new valid period, find the currently open record
            # and close it by setting its END_TMS to the new delta's START_TMS - 1 second.
            open_hist = df_history[
                (df_history["ASSET_ID"] == asset_id) &
                (df_history["DATA_PROVIDER"] == provider) &
                (df_history["END_TMS"].isna())
            ]
            if not open_hist.empty:
                closed_version = open_hist.iloc[0].copy()
                closed_version["END_TMS"] = start_tms - pd.to_timedelta(1, "s")
                closed_version["LAST_CHG_TMS"] = txn_start
                extra_versions.append(closed_version)
            # *** MODIFIED LOGIC END ***
            continue

        hist_row = hist_match.iloc[-1]  # latest txn for that valid start at the time

        # Identify which data columns actually change in this delta
        changed_cols = []
        for c in data_cols:
            if pd.notna(d[c]) and (d[c] != hist_row[c]):
                changed_cols.append(c)

        if not changed_cols:
            continue

        # Future contiguous rows with the same values (for changed columns) as hist_row
        future_rows = df_history[
            (df_history["ASSET_ID"] == asset_id) &
            (df_history["DATA_PROVIDER"] == provider) &
            (df_history["START_TMS"] > hist_row["START_TMS"])
        ].sort_values("START_TMS")

        contiguous = []
        for _, f in future_rows.iterrows():
            # Stop as soon as one of the changed columns differs from the "pre-change" value
            if any(f[c] != hist_row[c] for c in changed_cols):
                break
            contiguous.append(f)

        # Create new txn versions for those future rows (preserving their valid END_TMS)
        if contiguous:
            cont_df = pd.DataFrame(contiguous)
            for _, r in cont_df.iterrows():
                new_version = r.copy()
                for c in changed_cols:
                    new_version[c] = d[c]
                new_version["LAST_CHG_TMS"] = txn_start
                # Preserve the original valid END_TMS for that period
                new_version["END_TMS"] = r["END_TMS"]
                extra_versions.append(new_version)

    # Add propagated versions into delta set
    if extra_versions:
        df_delta = pd.concat([df_delta, pd.DataFrame(extra_versions)], ignore_index=True, sort=False)

    # --- 3) Attach the historical END_TMS to *all* delta rows that correspond to historical starts ---
    # (Rows that are truly new valid periods will remain END_TMS = NaT / None.)
    if "END_TMS" not in df_delta.columns:
        df_delta["END_TMS"] = pd.NaT
    # Map in END_TMS for any delta row with an existing historical START_TMS
    keys = list(zip(df_delta["ASSET_ID"], df_delta["DATA_PROVIDER"], df_delta["START_TMS"]))
    df_delta["END_TMS"] = [
        df_delta.loc[i, "END_TMS"] if pd.notna(df_delta.loc[i, "END_TMS"]) else hist_end_lookup.get(k, pd.NaT)
        for i, k in enumerate(keys)
    ]

    # --- 4) Unify history + delta (do NOT recompute END_TMS) ---
    keep_cols = [
        "ASSET_ID", "DATA_PROVIDER", "START_TMS", "END_TMS", "LAST_CHG_TMS",
        "DATA_PROVIDER_TYPE", "ASSET_COUNTRY", "ASSET_CURRENCY", "ASSET_PRICE", "ASSET_MATURITY_TMS"
    ]
    df_history_simple = df_history[keep_cols]
    df_unified = pd.concat([df_history_simple, df_delta[keep_cols]], ignore_index=True, sort=False)

    # Sort to establish chronology for forward-fill and TXN_END_TMS calc
    df_unified = df_unified.sort_values(by=["ASSET_ID", "DATA_PROVIDER", "START_TMS", "LAST_CHG_TMS"], kind="mergesort")

    # --- 5) Handle deletion sentinels and forward-fill only data columns (NOT END_TMS) ---
    sentinel_map = {
        "ASSET_COUNTRY": "$$DELETED$$",
        "ASSET_CURRENCY": "$$DELETED$$",
        "ASSET_PRICE": 1234567890.12345,
        "ASSET_MATURITY_TMS": datetime.datetime(1900, 1, 1),
    }
    df_ff = df_unified.copy()
    for col_name, sentinel_value in sentinel_map.items():
        if col_name in df_ff.columns:
            if df_ff[col_name].dtype == "object":
                df_ff.loc[df_ff[col_name] == sentinel_value, col_name] = pd.NA
            else:
                df_ff.loc[df_ff[col_name] == sentinel_value, col_name] = None

    # Forward-fill "unchanged" data fields inside each asset/provider timeline
    df_ff[data_cols] = df_ff.groupby(["ASSET_ID", "DATA_PROVIDER"], group_keys=False)[data_cols].ffill()
    df_ff["DATA_PROVIDER_TYPE"] = df_ff.groupby(["ASSET_ID", "DATA_PROVIDER"], group_keys=False)["DATA_PROVIDER_TYPE"].ffill()

    # --- 6) Drop duplicates by *valid* state (keep all txn versions!) ---
    # Important: We keep LAST_CHG_TMS so we do NOT collapse different transactions.
    # We dedup rows that are literally identical across both valid-time and data columns + LAST_CHG_TMS.
    df_ff = df_ff.drop_duplicates(
        subset=[
            "ASSET_ID", "DATA_PROVIDER", "START_TMS", "END_TMS", "LAST_CHG_TMS",
            "DATA_PROVIDER_TYPE", "ASSET_COUNTRY", "ASSET_CURRENCY", "ASSET_PRICE", "ASSET_MATURITY_TMS"
        ],
        keep="first"
    )

    # --- 7) Rebuild transaction-time fields (TXN_START/END) per valid start ---
    # TXN_START_TMS := LAST_CHG_TMS
    df_ff = df_ff.sort_values(by=["ASSET_ID", "DATA_PROVIDER", "START_TMS", "LAST_CHG_TMS"], kind="mergesort")
    df_ff["TXN_START_TMS"] = df_ff["LAST_CHG_TMS"]
    df_ff["TXN_END_TMS"] = df_ff.groupby(["ASSET_ID", "DATA_PROVIDER", "START_TMS"])["TXN_START_TMS"].shift(-1)
    df_ff["TXN_END_TMS"] = df_ff["TXN_END_TMS"] - pd.to_timedelta(1, unit="s")

    # --- 8) Derive bitemporal flags ---
    # IS_CURRENT is strictly based on valid-time END_TMS
    df_ff["IS_CURRENT"] = df_ff["END_TMS"].isna()
    # Latest transaction for a valid start has TXN_END_TMS = null
    df_ff["IS_LATEST_TXN"] = df_ff["TXN_END_TMS"].isna()

    # --- 9) Final column order and sentinel re-application for nulls (if desired) ---
    final_df = df_ff[[
        "ASSET_ID", "DATA_PROVIDER", "DATA_PROVIDER_TYPE",
        "START_TMS", "END_TMS",
        "TXN_START_TMS", "TXN_END_TMS",
        "IS_CURRENT", "IS_LATEST_TXN",
        "ASSET_COUNTRY", "ASSET_CURRENCY", "ASSET_PRICE", "ASSET_MATURITY_TMS"
    ]].reset_index(drop=True)

    # Note: we intentionally DO NOT transform NaT END_TMS to any sentinel.
    # Only data columns (country/currency/price/maturity) may need sentinel reapplication if you want.
    # Leaving them as true NULLs is generally best for Snowflake.

    return final_df
