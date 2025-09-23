def bi_temporal_merge_pandas(df_history: pd.DataFrame, df_delta: pd.DataFrame) -> pd.DataFrame:
    """
    Performs the core bitemporal logic using a simplified and robust ffill process.
    """
    if not df_history.empty:
        history_cols = df_history.columns.tolist()
    else:
        history_cols = df_delta.columns.tolist() + ['END_TMS', 'TXN_START_TMS', 'TXN_END_TMS', 'IS_CURRENT', 'IS_LATEST_TXN', 'MESH_INSRT_TS', 'MESH_LAST_UPDATE_TS', 'MESH_RUN_ID']
        history_cols = sorted(set(history_cols), key=history_cols.index)

    non_data_cols = {"ASSET_ID", "DATA_PROVIDER", "START_TMS", "END_TMS", "LAST_CHG_TMS", "TXN_START_TMS", "TXN_END_TMS", "IS_CURRENT", "IS_LATEST_TXN", "MESH_INSRT_TS", "MESH_LAST_UPDATE_TS", "MESH_RUN_ID"}
    data_cols = [c for c in history_cols if c not in non_data_cols]
    
    extra_versions = []
    df_delta = df_delta.sort_values("START_TMS")
    closed_open_records = set()

    def values_are_same(a, b):
        if pd.isna(a) and pd.isna(b): return True
        if pd.isna(a) or pd.isna(b): return False
        return a == b

    for _, d in df_delta.iterrows():
        asset_id, start_tms, txn_start = d["ASSET_ID"], d["START_TMS"], d["LAST_CHG_TMS"]
        hist_match = df_history[(df_history["ASSET_ID"] == asset_id) & (df_history["START_TMS"] == start_tms)].sort_values("LAST_CHG_TMS")
        if hist_match.empty:
            open_hist = df_history[(df_history["ASSET_ID"] == asset_id) & (df_history["END_TMS"].isna()) & (df_history["START_TMS"] < start_tms)].sort_values("LAST_CHG_TMS", ascending=False)
            if not open_hist.empty:
                open_record = open_hist.iloc[0]
                open_record_key = (asset_id, open_record["START_TMS"])
                if open_record_key not in closed_open_records:
                    new_txn_record = open_record.copy()
                    new_txn_record["LAST_CHG_TMS"] = txn_start
                    new_txn_record["END_TMS"] = start_tms - pd.to_timedelta(1, "s")
                    extra_versions.append(new_txn_record)
                    closed_open_records.add(open_record_key)
            continue
        hist_row = hist_match.iloc[-1]
        changed_cols = [c for c in data_cols if c in d and pd.notna(d[c]) and not values_are_same(d.get(c), hist_row.get(c))]
        if not changed_cols: continue
        future_rows = df_history[(df_history["ASSET_ID"] == asset_id) & (df_history["START_TMS"] > hist_row["START_TMS"])].sort_values("START_TMS")
        for _, f_row in future_rows.iterrows():
            inherited_cols = [c for c in changed_cols if values_are_same(f_row.get(c), hist_row.get(c))]
            if not inherited_cols: break
            new_version = f_row.copy()
            for c in inherited_cols: new_version[c] = d[c]
            new_version["LAST_CHG_TMS"] = txn_start
            extra_versions.append(new_version)
    if extra_versions: df_delta = pd.concat([df_delta, pd.DataFrame(extra_versions)], ignore_index=True, sort=False)

    if "END_TMS" not in df_delta.columns: df_delta["END_TMS"] = pd.NaT
    keys = list(zip(df_delta["ASSET_ID"], df_delta["START_TMS"]))
    df_delta["END_TMS"] = [
        df_delta.loc[i, "END_TMS"] if pd.notna(df_delta.loc[i, "END_TMS"]) else (df_history[(df_history["ASSET_ID"] == k[0]) & (df_history["START_TMS"] == k[1])]["END_TMS"].iloc[0] if not df_history[(df_history["ASSET_ID"] == k[0]) & (df_history["START_TMS"] == k[1])].empty else pd.NaT) for i, k in enumerate(keys)
    ]
    
    df_unified = pd.concat([df_history, df_delta], ignore_index=True)

    for col_name in df_unified.columns:
        if "TMS" in col_name and df_unified[col_name].dtype == 'object':
            df_unified[col_name] = pd.to_datetime(df_unified[col_name], errors='coerce')

    sentinel_map = {"ASSET_PRICE": 1234567890.12345, "ASSET_MATURITY_TMS": pd.to_datetime("1700-01-01")}
    group_cols = ["ASSET_ID"]
    df_unified = df_unified.sort_values(by=group_cols + ["START_TMS", "LAST_CHG_TMS"])
    
    # **SIMPLIFIED LOGIC for Deletions and Forward Fill**
    fill_cols = [c for c in data_cols + ["DATA_PROVIDER_TYPE"] if c in df_unified.columns]
    
    # 1. Perform the forward fill. This will correctly carry the sentinel values forward.
    if fill_cols:
        df_unified[fill_cols] = df_unified.groupby(group_cols, group_keys=False)[fill_cols].ffill()

    # 2. Now that the sentinels have been propagated, replace them with proper null values.
    for col, sentinel in sentinel_map.items():
        if col in df_unified.columns:
            mask = df_unified[col] == sentinel
            if pd.api.types.is_datetime64_any_dtype(df_unified[col]):
                df_unified.loc[mask, col] = pd.NaT
            else:
                df_unified.loc[mask, col] = pd.NA

    primary_key = ["ASSET_ID", "START_TMS", "LAST_CHG_TMS"]
    df_clean = df_unified.drop_duplicates(subset=primary_key, keep="last").copy()

    df_clean["TXN_START_TMS"] = df_clean["LAST_CHG_TMS"]
    df_clean["TXN_END_TMS"] = df_clean.groupby(["ASSET_ID", "START_TMS"])["TXN_START_TMS"].shift(-1) - pd.to_timedelta(1, "s")
    
    df_clean['IS_LATEST_TXN_TEMP'] = df_clean["TXN_END_TMS"].isna()
    df_clean = df_clean.sort_values(["ASSET_ID", "START_TMS"])
    future_starts = df_clean.groupby(["ASSET_ID"])["START_TMS"].shift(-1)
    needs_end = df_clean["END_TMS"].isna() & future_starts.notna() & df_clean['IS_LATEST_TXN_TEMP']
    df_clean.loc[needs_end, "END_TMS"] = future_starts[needs_end] - pd.to_timedelta(1, "s")
    df_clean = df_clean.drop(columns=['IS_LATEST_TXN_TEMP'])
    
    vt_group_cols = ["ASSET_ID", "START_TMS"]
    df_clean["IS_LATEST_TXN"] = df_clean["TXN_END_TMS"].isna()
    df_clean = df_clean.sort_values(by=vt_group_cols + ["LAST_CHG_TMS"])
    latest_end_tms_map = df_clean.groupby(vt_group_cols)["END_TMS"].transform('last')
    df_clean["IS_CURRENT"] = latest_end_tms_map.isna()

    final_df = df_clean.reindex(columns=history_cols).reset_index(drop=True)
    
    return final_df
