# fastdb_util

Exports FastDB light curves to [nested-pandas](https://nested-pandas.readthedocs.io/) Parquet files.

## Setup

```
pip install nested-pandas
```

`fastdb_client` is not on PyPI — clone [LSSTDESC/FASTDB](https://github.com/LSSTDESC/FASTDB) and add its `client/` directory to your Python path:

```bash
export PYTHONPATH=/path/to/FASTDB/client:$PYTHONPATH
```

Add this to `~/.zshrc` (or `~/.bashrc`) to make it permanent.

You also need `~/.fastdb.ini` with your credentials (permissions must be `chmod go-rwx ~/.fastdb.ini`).

## Usage

```python
from export import export
```

### Export everything

Calling with no search criteria returns all objects in the database:

```python
nf = export('out.parquet')
```

### Query by search criteria

Pass any [objectsearch](https://fastdb.readthedocs.io) parameters as keyword arguments:

```python
# Spatial cone search
nf = export('out.parquet', ra=52.5, dec=-27.5, radius=60)

# Time window (MJD)
nf = export('out.parquet', firstdet_mjd_min=61160, firstdet_mjd_max=61161)

# Combined
nf = export('out.parquet', ra=52.5, dec=-27.5, radius=60, firstdet_mjd_min=61000)
```

### Provide rootids directly

```python
nf = export('out.parquet', rootids=['1a671daa-...', '2f6f8a95-...'])
```

### Select columns

By default all columns returned by FastDB are included. Pass lists to restrict:

```python
nf = export(
    'out.parquet',
    firstdet_mjd_min=61160,
    base_columns=['rootid', 'ra', 'dec'],
    nested_columns=['mjd', 'flux', 'fluxerr', 'band'],
)
```

### Use a non-production environment

```python
nf = export('out.parquet', env='dev', firstdet_mjd_min=61160)
```

### Re-use an existing connection

```python
from fastdb_client import FASTDBClient
fdb = FASTDBClient('production')
nf = export('out.parquet', fdb=fdb, firstdet_mjd_min=61160)
```

### Chunked export

For large exports, use `chunk_size` or `mjd_bin_size` to write one parquet file per chunk into a directory instead of loading everything into memory at once.

**Count-based chunks** — splits rootids into groups of N:

```python
path = export('out_dir/', chunk_size=1000)
# → out_dir/chunk_0000.parquet, chunk_0001.parquet, …
```

**MJD-binned chunks** — groups objects by `firstdet_mjd` window:

```python
path = export('out_dir/', mjd_bin_size=1.0)   # one file per day
# → out_dir/mjd_61160_61161.parquet, mjd_61161_61162.parquet, …
```

When chunking, `export` returns the output directory path (a string) rather than a `NestedFrame`, so the full dataset is never loaded into memory. Read individual files as needed:

```python
from nested_pandas import read_parquet
nf = read_parquet('out_dir/mjd_61160_61161.parquet')
```

## Output

`export` returns a `NestedFrame` (single-file) or a directory path (chunked). Each row is one object; the `lightcurve` column contains a nested per-visit table with columns including `mjd`, `flux`, `fluxerr`, `band`, `isdet`, `ispatch`, and per-source positions (`det_ra`, `det_dec`, …).

```python
from nested_pandas import read_parquet
nf = read_parquet('out.parquet')
nf['lightcurve']   # nested per-visit data
```

See `plot_lightcurves.ipynb` for an example of reading and plotting the output.
