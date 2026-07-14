import pandas as pd
from nested_pandas import NestedFrame
from fastdb_client import FASTDBClient


def fetch_rootids(fdb, **query_kwargs):
    result = fdb.post("/objectsearch/realtime", json=query_kwargs)
    return result["rootid"]


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


def export(
    output_path,
    fdb=None,
    env="production",
    rootids=None,
    base_columns=None,
    nested_columns=None,
    **query_kwargs,
):
    if fdb is None:
        fdb = FASTDBClient(env)

    if rootids is None:
        rootids = fetch_rootids(fdb, **query_kwargs)

    response = fetch_lightcurves(fdb, rootids)
    nf = build_nested_frame(response, base_columns=base_columns, nested_columns=nested_columns)
    nf.to_parquet(output_path)
    return nf
