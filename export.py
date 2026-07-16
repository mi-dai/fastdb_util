import logging
import math
import os
import pandas as pd
from nested_pandas import NestedFrame, read_parquet
from fastdb_client import FASTDBClient

logger = logging.getLogger(__name__)


def fetch_rootids(fdb, **query_kwargs):
    return fdb.post("/objectsearch/realtime", json=query_kwargs)


def fetch_lightcurves(fdb, rootids=None, limit=None, offset=None):
    body = {"include_source_positions": True, "return_object_info": True}
    if rootids is not None:
        body["objids"] = rootids
    else:
        if limit is not None:
            body["limit"] = limit
        if offset is not None:
            body["offset"] = offset
    return fdb.post("/ltcv/getmanyltcvs/realtime", json=body)


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
    _INT64_COLUMNS = {
        "diasourceid", "diaforcedsourceid", "source_diaobjectid",
        "forced_diaobjectid", "visit",
    }
    for col in flat_df.columns:
        if col in _INT64_COLUMNS:
            flat_df[col] = flat_df[col].astype(pd.Int64Dtype())
        elif flat_df[col].dtype == "float64":
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
    response = fetch_lightcurves(fdb, rootids=rootids)
    nf = build_nested_frame(response, base_columns=base_columns, nested_columns=nested_columns)
    nf.to_parquet(path)


def export(
    output_path,
    fdb=None,
    env="production",
    rootids=None,
    base_columns=None,
    nested_columns=None,
    chunk_size=1000,
    mjd_bin_size=None,
    log_every=100,
    bypass_object_search=False,
    max_objects=None,
    **query_kwargs,
):
    if fdb is None:
        fdb = FASTDBClient(env)

    if bypass_object_search:
        if chunk_size > 1000:
            logger.warning("chunk_size %d > 1000 may not work as expected with bypass_object_search", chunk_size)
        os.makedirs(output_path, exist_ok=True)
        offset = 0
        chunk_idx = 0
        saved = 0
        while True:
            limit = chunk_size if max_objects is None else min(chunk_size, max_objects - saved)
            response = fetch_lightcurves(fdb, limit=limit, offset=offset)
            if not response.get("ltcvs"):
                break
            nf = build_nested_frame(response, base_columns=base_columns, nested_columns=nested_columns)
            nf.to_parquet(os.path.join(output_path, f"chunk_{chunk_idx:04d}.parquet"))
            saved += len(nf)
            logger.info("Saved %d objects so far (chunk %d)", saved, chunk_idx)
            if max_objects is not None and saved >= max_objects:
                break
            offset += chunk_size
            chunk_idx += 1
        return output_path

    search_result = None
    if rootids is None:
        search_result = fetch_rootids(fdb, **query_kwargs)
        rootids = search_result["rootid"]

    if max_objects is not None:
        rootids = rootids[:max_objects]
        if search_result is not None:
            for key in search_result:
                if isinstance(search_result[key], list):
                    search_result[key] = search_result[key][:max_objects]

    total = len(rootids)
    logger.info("Total objects to export: %d", total)

    # Single-file path
    if (mjd_bin_size is None and total <= chunk_size) or total == 0:
        if total == 0:
            return NestedFrame()
        _write_chunk(fdb, rootids, output_path, base_columns, nested_columns)
        logger.info("Saved %d/%d objects", total, total)
        return read_parquet(output_path)

    os.makedirs(output_path, exist_ok=True)
    saved = 0

    if mjd_bin_size is not None and search_result is not None:
        # Group rootids by firstdet_mjd bin, then sub-chunk by count if needed
        mjds = search_result["firstdet_mjd"]
        bins = {}
        for rid, mjd in zip(rootids, mjds):
            bin_start = math.floor(mjd / mjd_bin_size) * mjd_bin_size
            bins.setdefault(bin_start, []).append(rid)
        for bin_start, bin_rids in sorted(bins.items()):
            bin_end = bin_start + mjd_bin_size
            sub_chunks = [bin_rids[i:i + chunk_size] for i in range(0, len(bin_rids), chunk_size)]
            for j, chunk in enumerate(sub_chunks):
                if len(sub_chunks) == 1:
                    fname = f"mjd_{bin_start:.0f}_{bin_end:.0f}.parquet"
                else:
                    fname = f"mjd_{bin_start:.0f}_{bin_end:.0f}_{j:04d}.parquet"
                _write_chunk(fdb, chunk, os.path.join(output_path, fname), base_columns, nested_columns)
                saved += len(chunk)
                if log_every and (saved % log_every < len(chunk) or saved == total):
                    logger.info("Saved %d/%d objects", saved, total)
    else:
        # Count-based chunking
        chunks = [rootids[i:i + chunk_size] for i in range(0, len(rootids), chunk_size)]
        for i, chunk in enumerate(chunks):
            fname = f"chunk_{i:04d}.parquet"
            _write_chunk(fdb, chunk, os.path.join(output_path, fname), base_columns, nested_columns)
            saved += len(chunk)
            if log_every and (saved % log_every < len(chunk) or saved == total):
                logger.info("Saved %d/%d objects", saved, total)

    return output_path
