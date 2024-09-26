"""Parquet to JSON conversion utility."""
from pathlib import Path
import polars as pl
from polars.exceptions import PolarsError


class Parquet2JSONError(Exception):
    """Base class for exceptions in this module."""


def read_parquet(path: str) -> pl.LazyFrame:
    """Read a parquet file and return a LazyFrame."""
    print(f"Reading parquet file from {path}")
    lf = pl.scan_parquet(path)
    return lf


def write_json(lf: pl.LazyFrame, path: Path) -> None:
    """Write a LazyFrame to a JSON file."""
    lf.sink_ndjson(path)


def convert(parquet_in: str, json_out: Path) -> None:
    """Convert a parquet file to a JSON file."""
    try:
        lf = read_parquet(parquet_in)
        write_json(lf, json_out)
    except (PolarsError, FileNotFoundError) as e:
        raise Parquet2JSONError(e) from e
