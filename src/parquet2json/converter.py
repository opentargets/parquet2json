"""Parquet to JSON conversion utility."""

import json
from logging import Logger
from pathlib import Path
from typing import Any
import polars as pl
import polars.selectors as cs
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

    def get_pyarrow_schema(self, parquet_path: Path) -> pa.Schema:
        """Get the schema of a parquet file using pyarrow.
        Currently this is required because of issues with Polars and
        reading list[struct] columns.
        """
        schema = (
            pl.read_parquet(
                parquet_path,
                n_rows=1,
                hive_partitioning=True,
            )
            .to_arrow()
            .schema
        )
        # Workaround for chromosome datatype inference from hive
        if "chromosome" in schema.names:
            schema = schema.set(
                schema.get_field_index("chromosome"),
                pa.field("chromosome", pa.string()),
            )
        self.log.debug("Schema: %s", schema)
        return schema

    def read_parquet(self, parquet_path: Path) -> pl.DataFrame:
        """Read a parquet file and return a DataFrame."""
        try:
            # pyarrow does not automatically detect the filesystem
            filesystem, path = FileSystem.from_uri(parquet_path)
            schema = self.get_pyarrow_schema(parquet_path)
            df = pl.read_parquet(
                path,
                hive_partitioning=True,
                use_pyarrow=True,
                pyarrow_options={"filesystem": filesystem, "schema": schema},
            ).lazy()
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
        self, collection: dict[str, Any] | list[Any]
    ) -> dict[str, Any]:
        """Recursively drop null values from a Dict or List."""
        if isinstance(collection, list):
            return type(collection)(
                self.drop_nulls_from_collection(x) for x in collection if x is not None
            )
        elif isinstance(collection, dict):
            return type(collection)(
                (self.drop_nulls_from_collection(k), self.drop_nulls_from_collection(v))
                for k, v in collection.items()
                if k is not None and v is not None
            )
        else:
            return collection

    def json_lines_to_stdout(self, rows: list[dict[str, Any]]) -> None:
        """Write a list of rows to stdout as JSON lines."""
        return [
            sys.stdout.write(json.dumps(self.drop_nulls_from_collection(row)) + "\n")
            for row in rows
        ]

    def json_lines_to_file(self, rows: list[dict[str, Any]], path: Path) -> None:
        """Write a list of rows to a file as JSON lines."""
        with open(path, "w", encoding="UTF-8") as f:
            return [
                f.write(json.dumps(self.drop_nulls_from_collection(row)) + "\n")
                for row in rows
            ]

    def drop_rows_with_infinite_values(self, df: pl.DataFrame) -> pl.DataFrame:
        """Drop rows with infinite values from the DataFrame."""
        return df.filter(~pl.any_horizontal(cs.numeric().is_infinite()))

    def write_json(self, df: pl.DataFrame, path: Path) -> None:
        """Write the DataFrame as JSON"""
        filtered_df = self.drop_rows_with_infinite_values(df)
        rows = filtered_df.collect().iter_rows(named=True)
        if path is None:
            self.json_lines_to_stdout(rows)
        else:
            self.json_lines_to_file(rows, path)


def convert(parquet_path: Path, json_path: Path, log: Logger) -> None:
    """Convert a parquet file to a JSON file."""
    try:
        converter = Converter(log)
        log.debug("Reading %s", parquet_path)
        df = converter.read_parquet(parquet_path)
        log.debug("Writing %s", json_path)
        converter.write_json(df, json_path)
    except PolarsError as e:
        raise Parquet2JSONError(e) from e
