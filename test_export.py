import glob
import os
import shutil

import pandas as pd
import pytest

from nested_pandas import NestedFrame, read_parquet
from fastdb_client import FASTDBClient

from export import export, fetch_rootids

MJD_MIN = 61150
MJD_MAX = 61161
MJD_SINGLE_DAY_MIN = 61160
MJD_SINGLE_DAY_MAX = 61161

TEST_OUTPUT = os.path.join(os.path.dirname(__file__), "test_output")


def output_path(*parts):
    path = os.path.join(TEST_OUTPUT, *parts)
    os.makedirs(os.path.dirname(path) if "." in os.path.basename(path) else path, exist_ok=True)
    return path


@pytest.fixture(scope="module")
def fdb():
    return FASTDBClient("production")


@pytest.fixture(scope="module")
def single_file(fdb):
    path = output_path("single", "out.parquet")
    return export(path, fdb=fdb, firstdet_mjd_min=MJD_SINGLE_DAY_MIN, firstdet_mjd_max=MJD_SINGLE_DAY_MAX)


@pytest.fixture(scope="module")
def chunked_dir(fdb):
    path = output_path("chunked")
    shutil.rmtree(path, ignore_errors=True)
    return export(path, fdb=fdb, firstdet_mjd_min=MJD_MIN, firstdet_mjd_max=MJD_MAX, chunk_size=3)


@pytest.fixture(scope="module")
def mjd_binned_dir(fdb):
    path = output_path("mjd_binned")
    shutil.rmtree(path, ignore_errors=True)
    return export(path, fdb=fdb, firstdet_mjd_min=MJD_MIN, firstdet_mjd_max=MJD_MAX, mjd_bin_size=1.0)


# --- single-file export ---

def test_single_file_returns_nested_frame(single_file):
    assert isinstance(single_file, NestedFrame)

def test_single_file_has_objects(single_file):
    assert len(single_file) > 0

def test_single_file_has_lightcurve_column(single_file):
    assert "lightcurve" in single_file.columns

def test_single_file_has_position_columns(single_file):
    assert "ra" in single_file.columns
    assert "dec" in single_file.columns

def test_single_file_nested_has_mjd_flux_band(single_file):
    lc = pd.DataFrame(single_file.iloc[0]["lightcurve"])
    assert {"mjd", "flux", "fluxerr", "band"}.issubset(lc.columns)


# --- count-based chunked export ---

def test_chunked_returns_path(chunked_dir):
    assert isinstance(chunked_dir, str)

def test_chunked_files_exist(chunked_dir):
    files = os.listdir(chunked_dir)
    assert any(f.startswith("chunk_") and f.endswith(".parquet") for f in files)

def test_chunked_readable(chunked_dir):
    files = sorted(glob.glob(os.path.join(chunked_dir, "*.parquet")))
    assert len(files) > 0
    for f in files:
        nf = read_parquet(f)
        assert isinstance(nf, NestedFrame)
        assert len(nf) > 0


# --- MJD-binned export ---

def test_mjd_binned_returns_path(mjd_binned_dir):
    assert isinstance(mjd_binned_dir, str)

def test_mjd_binned_files_named_by_mjd(mjd_binned_dir):
    files = os.listdir(mjd_binned_dir)
    assert all(f.startswith("mjd_") and f.endswith(".parquet") for f in files)

def test_mjd_binned_readable(mjd_binned_dir):
    nf = read_parquet(mjd_binned_dir)
    assert isinstance(nf, NestedFrame)
    assert len(nf) > 0

def test_mjd_binned_column_filter(fdb):
    path = output_path("col_filter")
    shutil.rmtree(path, ignore_errors=True)
    export(
        path, fdb=fdb,
        firstdet_mjd_min=MJD_MIN, firstdet_mjd_max=MJD_MAX,
        mjd_bin_size=1.0,
        base_columns=["rootid", "ra", "dec"],
        nested_columns=["mjd", "flux", "fluxerr", "band"],
    )
    nf = read_parquet(path)
    assert set(nf.columns) == {"rootid", "ra", "dec", "lightcurve"}
    lc = pd.DataFrame(nf.iloc[0]["lightcurve"])
    assert set(lc.columns) == {"mjd", "flux", "fluxerr", "band"}


# --- MJD-binned + count sub-chunked export ---

def test_mjd_binned_with_chunk_size(fdb):
    path = output_path("mjd_chunked")
    shutil.rmtree(path, ignore_errors=True)
    export(path, fdb=fdb, firstdet_mjd_min=MJD_MIN, firstdet_mjd_max=MJD_MAX, mjd_bin_size=1.0, chunk_size=3)
    files = os.listdir(path)
    assert all(f.startswith("mjd_") and f.endswith(".parquet") for f in files)
    # bins with >3 objects should produce sub-chunk files (e.g. mjd_61150_61151_0000.parquet)
    sub_chunked = [f for f in files if f.count("_") == 4]
    single = [f for f in files if f.count("_") == 3]
    assert len(sub_chunked) > 0 or len(single) > 0
    for f in sorted(files):
        nf = read_parquet(os.path.join(path, f))
        assert len(nf) > 0

# --- bypass_object_search ---

def test_bypass_object_search(fdb):
    path = output_path("bypass")
    shutil.rmtree(path, ignore_errors=True)
    result = export(path, fdb=fdb, bypass_object_search=True, chunk_size=5, max_objects=10)
    assert isinstance(result, str)
    files = sorted(glob.glob(os.path.join(path, "chunk_*.parquet")))
    assert len(files) == 2
    for f in files:
        nf = read_parquet(f)
        assert len(nf) > 0

def test_max_objects_objectsearch(fdb):
    path = output_path("max_objects", "out.parquet")
    nf = export(path, fdb=fdb, firstdet_mjd_min=MJD_MIN, firstdet_mjd_max=MJD_MAX, max_objects=3)
    assert isinstance(nf, NestedFrame)
    assert len(nf) == 3

# --- explicit rootids ---

def test_explicit_rootids(fdb):
    result = fetch_rootids(fdb, firstdet_mjd_min=MJD_SINGLE_DAY_MIN, firstdet_mjd_max=MJD_SINGLE_DAY_MAX)
    rootids = result["rootid"][:3]
    path = output_path("explicit", "out.parquet")
    nf = export(path, fdb=fdb, rootids=rootids)
    assert isinstance(nf, NestedFrame)
    assert len(nf) == 3


# --- empty result ---

def test_empty_result_returns_empty_frame(fdb):
    path = output_path("empty", "out.parquet")
    nf = export(path, fdb=fdb, firstdet_mjd_min=99999, firstdet_mjd_max=99999)
    assert isinstance(nf, NestedFrame)
    assert len(nf) == 0
