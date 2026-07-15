import math
import os
import pandas as pd
from nested_pandas import NestedFrame,read_parquet
from fastdb_client import FASTDBClient


def fetch_rootids(fdb, **query_kwargs):
    return fdb.post("/objectsearch/realtime", json=query_kwargs)


def fetch_lightcurves(fdb, rootids):
    return fdb.post(
        "/ltcv/getmanyltcvs/realtime",
        json={
            "objids": rootids,
            "include_source_positions": True,
            "return_object_info": True,
        },
    )


def build_nested_frame(response, base_columns=None, nested_columns=None):
    objinfo_df = pd.DataFrame(response["objinfo"])

    visit_dfs = []
    for ltcv in response["ltcvs"]:
        rootid = ltcv["rootid"]
        df = pd.DataFrame({k: v for k, v in ltcv.items() if k != "rootid"})
        df["rootid"] = rootid
        visit_dfs.append(df)

    flat_df = pd.concat(visit_dfs, ignore_index=True).merge(
        objinfo_df, on="rootid", how="left"
    )

    # Preserve integer ID columns as nullable int64 so parquet schema is
    # consistent across chunks even when all values in a chunk are null.
    for col in flat_df.columns:
        if flat_df[col].dtype == "float64":
            non_null = flat_df[col].dropna()
            if len(non_null) > 0 and (non_null % 1 == 0).all():
                flat_df[col] = flat_df[col].astype(pd.Int64Dtype())

    all_obj_cols = list(objinfo_df.columns)
    all_visit_cols = [c for c in flat_df.columns if c not in all_obj_cols]

    if base_columns is None:
        base_columns = all_obj_cols
    if nested_columns is None:
        nested_columns = all_visit_cols

    return NestedFrame.from_flat(
        flat_df,
        base_columns=base_columns,
        nested_columns=nested_columns,
        on="rootid",
        name="lightcurve",
    )


def _write_chunk(fdb, rootids, path, base_columns, nested_columns):
    response = fetch_lightcurves(fdb, rootids)
    nf = build_nested_frame(response, base_columns=base_columns, nested_columns=nested_columns)
    pd.DataFrame.to_parquet(nf, path)


def export(
    output_path,
    fdb=None,
    env="production",
    rootids=None,
    base_columns=None,
    nested_columns=None,
    chunk_size=1000,
    mjd_bin_size=None,
    **query_kwargs,
):
    if fdb is None:
        fdb = FASTDBClient(env)

    search_result = None
    if rootids is None:
        search_result = fetch_rootids(fdb, **query_kwargs)
        rootids = search_result["rootid"]

    # Single-file path
    if (mjd_bin_size is None and len(rootids) <= chunk_size) or len(rootids) == 0:
        if len(rootids) == 0:
            return NestedFrame()
        _write_chunk(fdb, rootids, output_path, base_columns, nested_columns)
        return read_parquet(output_path)

    os.makedirs(output_path, exist_ok=True)

    if mjd_bin_size is not None and search_result is not None:
        # Group rootids by firstdet_mjd bin
        mjds = search_result["firstdet_mjd"]
        bins = {}
        for rid, mjd in zip(rootids, mjds):
            bin_start = math.floor(mjd / mjd_bin_size) * mjd_bin_size
            bins.setdefault(bin_start, []).append(rid)
        for bin_start, chunk in sorted(bins.items()):
            bin_end = bin_start + mjd_bin_size
            fname = f"mjd_{bin_start:.0f}_{bin_end:.0f}.parquet"
            _write_chunk(fdb, chunk, os.path.join(output_path, fname), base_columns, nested_columns)
    else:
        # Count-based chunking
        chunks = [rootids[i:i + chunk_size] for i in range(0, len(rootids), chunk_size)]
        for i, chunk in enumerate(chunks):
            fname = f"chunk_{i:04d}.parquet"
            _write_chunk(fdb, chunk, os.path.join(output_path, fname), base_columns, nested_columns)

    return output_path
