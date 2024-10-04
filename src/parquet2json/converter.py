"""Parquet to JSON conversion utility."""
from pathlib import Path
import polars as pl
from pyarrow import fs
from polars.exceptions import PolarsError


class Parquet2JSONError(Exception):
    """Base class for exceptions in this module."""


def read_parquet(parquet_path: Path) -> pl.DataFrame:
    """Read a parquet file and return a DataFrame."""
    try:
        # pyarrow does not automatically detect the filesystem
        filesystem, path = fs.FileSystem.from_uri(parquet_path)
        df = pl.read_parquet(path,
                             use_pyarrow=True,
                             pyarrow_options={"filesystem": filesystem})
        return df
    except FileNotFoundError as e:
        raise Parquet2JSONError(f"Couldn't find {parquet_path}") from e


def write_json(df: pl.DataFrame, path: Path) -> None:
    """Write a DataFrame to a JSON file."""
    df.write_ndjson(path)


def convert(parquet_path: Path, json_path: Path) -> None:
    """Convert a parquet file to a JSON file."""
    try:
        df = read_parquet(parquet_path)
        write_json(df, json_path)
    except (PolarsError) as e:
        raise Parquet2JSONError(e) from e
