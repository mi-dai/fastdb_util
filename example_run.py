#!/usr/bin/env python
"""
Export FastDB light curves to nested-pandas Parquet files.

Usage examples:

  # MJD-binned export with 30-day bins (default)
  python example_run.py out_dir/

  # MJD-binned export starting from a given MJD, with 1-day bins
  python example_run.py out_dir/ --mjd-bin-size 1.0 --firstdet-mjd-min 61160

  # Export all objects in the database (bypasses objectsearch materialized view)
  python example_run.py out_dir/ --bypass-object-search --chunk-size 1000

  # Quick test run capped at 500 objects
  python example_run.py out_dir/ --bypass-object-search --chunk-size 100 --max-objects 500

  # Write logs to a file
  python example_run.py out_dir/ --bypass-object-search --log-file export.log
"""
import sys
import argparse
import logging

sys.path.insert(0, '/global/homes/m/mdai/fastdb_util')


def main():
    parser = argparse.ArgumentParser(description='MJD-binned chunked export')
    parser.add_argument('out_dir', nargs='?', default='out_dir/',
                        help='Output directory (default: out_dir/)')
    parser.add_argument('--mjd-bin-size', type=float, default=30.0,
                        help='MJD bin size in days (default: 30.0)')
    parser.add_argument('--log-every', type=int, default=500,
                        help='Log progress every N objects (default: 500)')
    parser.add_argument('--chunk-size', type=int, default=5000,
                        help='Chunk size (default: 5000)')
    parser.add_argument('--firstdet-mjd-min', type=float, default=None,
                        help='Minimum MJD of first detection (default: None)')
    parser.add_argument('--bypass-object-search', action='store_true',
                        help='Bypass objectsearch and paginate getmanyltcvs directly (exports all objects)')
    parser.add_argument('--max-objects', type=int, default=None,
                        help='Cap total objects exported (default: no limit)')
    parser.add_argument('--log-file', default=None,
                        help='Log file name (default: log to stdout)')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        filename=args.log_file,
        format='%(asctime)s %(levelname)s %(message)s',
    )

    from export import export

    kwargs = dict(
        chunk_size=args.chunk_size,
        log_every=args.log_every,
        bypass_object_search=args.bypass_object_search,
        max_objects=args.max_objects,
    )
    if not args.bypass_object_search:
        kwargs['mjd_bin_size'] = args.mjd_bin_size
        if args.firstdet_mjd_min:
            kwargs['firstdet_mjd_min'] = args.firstdet_mjd_min
    path = export(args.out_dir, **kwargs)
    logging.info(f'Export complete: {path}')


if __name__ == '__main__':
    main()