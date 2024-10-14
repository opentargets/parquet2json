"""Parquet to JSON conversion utility."""
import json
from logging import Logger
from pathlib import Path
from typing import Any
import polars as pl
import pyarrow as pa
from pyarrow.fs import FileSystem
import sys
from polars.exceptions import PolarsError


class Parquet2JSONError(Exception):
    """Base class for exceptions in this module."""


class Converter:
    """Parquet to JSON converter class."""
    def __init__(self, log: Logger):
        self.log = log

    def read_parquet(self, parquet_path: Path) -> pl.DataFrame:
        """Read a parquet file and return a DataFrame."""
        try:
            # pyarrow does not automatically detect the filesystem
            pl.Config(set_auto_structify=True)
            filesystem, path = FileSystem.from_uri(parquet_path)
            pl_schema = pl.scan_parquet(parquet_path).schema
            self.log.debug("Polar Schema: %s", pl_schema)
            schema: pa.Schema = pl.read_parquet(parquet_path,
                                                n_rows=1,
                                                hive_partitioning=True,
                                                use_pyarrow=False,
                                                pyarrow_options={"filesystem": filesystem}
                                                ).to_arrow().schema
            self.log.debug("Schema: %s", schema)
            df = pl.read_parquet(path,
                                 hive_partitioning=True,
                                 use_pyarrow=True,
                                 pyarrow_options={"filesystem": filesystem,
                                                  "schema": schema}
                                 )
            return df
        except FileNotFoundError as e:
            self.log.error(f"Couldn't find {parquet_path}")
            raise Parquet2JSONError(f"Couldn't find {parquet_path}") from e
        except pa.ArrowInvalid as e:
            self.log.error(f"Couldn't read {parquet_path}")
            raise Parquet2JSONError(f"Couldn't read {parquet_path}") from e
        except PolarsError as e:
            self.log.error(f"Polar error reading {parquet_path}")
            raise Parquet2JSONError(e) from e

    def drop_nulls_from_collection(
        self,
        collection: dict[str, Any] | list[Any]
    ) -> dict[str, Any]:
        """Recursively drop null values from a Dict or List.
        """
        if isinstance(collection, list):
            return type(collection)(
                self.drop_nulls_from_collection(x)
                for x in collection if x is not None)
        elif isinstance(collection, dict):
            return type(collection)(
                (self.drop_nulls_from_collection(k),
                 self.drop_nulls_from_collection(v))
                for k, v in collection.items()
                if k is not None and v is not None)
        else:
            return collection

    def json_lines_to_stdout(
        self,
        rows: list[dict[str, Any]]
    ) -> None:
        """Write a list of rows to stdout as JSON lines."""
        return [sys.stdout.write(
            json.dumps(
                self.drop_nulls_from_collection(row)) + '\n')
                for row in rows]

    def json_lines_to_file(
        self,
        rows: list[dict[str, Any]],
        path: Path
    ) -> None:
        """Write a list of rows to a file as JSON lines."""
        with open(path, 'w', encoding='UTF-8') as f:
            return [f.write(
                json.dumps(
                    self.drop_nulls_from_collection(row)) + '\n')
                    for row in rows]

    def write_json(self, df: pl.DataFrame, path: Path) -> None:
        """Write the DataFrame as JSON"""
        rows = df.iter_rows(named=True)
        if path is None:
            self.json_lines_to_stdout(rows)
        else:
            self.json_lines_to_file(rows, path)


def convert(
    parquet_path: Path,
    json_path: Path,
    log: Logger
) -> None:
    """Convert a parquet file to a JSON file."""
    try:
        converter = Converter(log)
        log.debug("Reading %s", parquet_path)
        df = converter.read_parquet(parquet_path)
        log.debug("Writing %s", json_path)
        converter.write_json(df, json_path)
    except (PolarsError) as e:
        raise Parquet2JSONError(e) from e
