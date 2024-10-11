"""Parquet to JSON conversion utility."""
import json
from logging import Logger
from pathlib import Path
from typing import Any
import polars as pl
from pyarrow import fs, ArrowInvalid
import pyarrow.dataset as ds
import sys
from polars.exceptions import PolarsError


class Parquet2JSONError(Exception):
    """Base class for exceptions in this module."""


def read_parquet(parquet_path: Path, log) -> pl.DataFrame:
    """Read a parquet file and return a DataFrame."""
    try:
        # pyarrow does not automatically detect the filesystem
        pl.Config(set_auto_structify=True)
        filesystem, path = fs.FileSystem.from_uri(parquet_path)
        #schema = pl.scan_parquet(parquet_path, hive_partitioning=True).collect_schema().to_python()
        schema = pl.read_parquet(parquet_path, n_rows=1, hive_partitioning=True, use_pyarrow=False, pyarrow_options={"filesystem": filesystem}).to_arrow().schema
        log.debug("Schema: %s", schema)
        df = pl.scan_pyarrow_dataset(ds.dataset(path, schema=schema, partitioning="hive", filesystem=filesystem, format="parquet")).collect()
        log.debug("Schema: %s", df.schema)
        #df = pl.read_parquet(path,
        #                     hive_partitioning=True,
        #                     use_pyarrow=True,
        #                     pyarrow_options={"filesystem": filesystem})
        return df
    except FileNotFoundError as e:
        log.error(f"Couldn't find {parquet_path}")
        raise Parquet2JSONError(f"Couldn't find {parquet_path}") from e
    except ArrowInvalid as e:
        log.error(f"Couldn't read {parquet_path}")
        raise Parquet2JSONError(f"Couldn't read {parquet_path}") from e
    except PolarsError as e:
        log.error(f"Polar error reading {parquet_path}")
        raise Parquet2JSONError(e) from e


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


def convert(parquet_path: Path, json_path: Path, log: Logger) -> None:
    """Convert a parquet file to a JSON file."""
    try:
        log.debug("Reading %s", parquet_path)
        df = read_parquet(parquet_path, log)
        log.debug("Writing %s", json_path)
        write_json(df, json_path)
    except (PolarsError) as e:
        raise Parquet2JSONError(e) from e
