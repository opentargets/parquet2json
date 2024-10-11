"""Parquet to JSON conversion utility."""
import json
from pathlib import Path
from typing import Any
import polars as pl
from pyarrow import fs, ArrowInvalid
import sys
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
    except ArrowInvalid as e:
        raise Parquet2JSONError(f"Couldn't read {parquet_path}") from e


def drop_nulls_from_row(row: dict[str, Any]) -> dict[str, Any]:
    """Drop null values from a row.
    """
    return {k: v for k, v in row.items() if v is not None}


def json_lines_to_stdout(rows: list[dict[str, Any]]) -> None:
    """Write a list of rows to stdout as JSON lines."""
    return [sys.stdout.write(json.dumps(drop_nulls_from_row(row)) + '\n')
            for row in rows]


def json_lines_to_file(rows: list[dict[str, Any]], path: Path) -> None:
    """Write a list of rows to a file as JSON lines."""
    with open(path, 'w', encoding='UTF-8') as f:
        return [f.write(json.dumps(drop_nulls_from_row(row)) + '\n')
                for row in rows]


def write_json(df: pl.DataFrame, path: Path) -> None:
    """Write the DataFrame as JSON"""
    rows = df.iter_rows(named=True)
    if path is None:
        json_lines_to_stdout(rows)
    else:
        json_lines_to_file(rows, path)


def convert(parquet_path: Path, json_path: Path) -> None:
    """Convert a parquet file to a JSON file."""
    try:
        df = read_parquet(parquet_path)
        write_json(df, json_path)
    except (PolarsError) as e:
        raise Parquet2JSONError(e) from e
