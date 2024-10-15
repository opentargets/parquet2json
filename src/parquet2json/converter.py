"""Parquet to JSON conversion utility."""

import json
import sys
from logging import Logger
from pathlib import Path
from typing import Any, Iterator

import polars as pl
import pyarrow as pa
from polars.exceptions import PolarsError
from pyarrow.fs import FileSystem


class Parquet2JSONError(Exception):
    """Base class for exceptions in this module."""


class Converter:
    """Parquet to JSON converter class."""

    def __init__(self, log: Logger):
        self.log = log

    def get_pyarrow_schema(
        self,
        path: Path,
    ) -> pa.Schema:
        """Get the schema of a parquet file using pyarrow.
        Currently this is required because of issues with Polars and
        reading list[struct] columns.
        """
        schema = (
            pl.read_parquet(
                path,
                hive_partitioning=True,
            )
            .to_arrow()
            .schema
        )
        if "chromosome" in schema.names:
            self.log.debug("Casting chromosome to string")
            schema = schema.set(
                schema.get_field_index("chromosome"),
                pa.field("chromosome", pa.string()),
            )
        # Workaround for log2h4h3
        if "log2h4h3" in schema.names:
            self.log.debug("Removing log2h4h3")
            schema = schema.remove(schema.get_field_index("log2h4h3"))
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
                pyarrow_options={
                    "filesystem": filesystem,
                    "schema": schema,
                },
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

    def write_json(self, df: pl.DataFrame, path: Path) -> None:
        """Write the DataFrame as line-delimited JSON"""
        rows = df.iter_rows(named=True)
        if path is None:
            self._json_lines_to_stdout(rows)
        else:
            self._json_lines_to_file(rows, path)

    def _drop_nulls_recursively(
        self, collection: dict[str, Any] | list[Any]
    ) -> dict[str, Any]:
        """Recursively drop null values from a Dict or List."""
        if isinstance(collection, list):
            return [
                self._drop_nulls_recursively(x) for x in collection if x is not None
            ]
        if isinstance(collection, dict):
            return {
                k: (self._drop_nulls_recursively(v))
                for k, v in collection.items()
                if v is not None
            }
        return collection

    def _serialize_rows(self, rows: Iterator[dict[str, Any]]) -> Iterator[str]:
        """Serialize a row to a JSON string."""
        for row in rows:
            formatted_row = "%s\n" % json.dumps(self._drop_nulls_recursively(row))
            yield formatted_row

    def _json_lines_to_stdout(self, rows: Iterator[dict[str, Any]]) -> None:
        """Write a list of rows to stdout as JSON lines."""
        sys.stdout.writelines(self._serialize_rows(rows))

    def _json_lines_to_file(self, rows: Iterator[dict[str, Any]], path: Path) -> None:
        """Write a list of rows to a file as JSON lines."""
        with open(path, "w", encoding="UTF-8") as f:
            f.writelines(self._serialize_rows(rows))


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
