"""CLI entry point for the application."""
from pathlib import Path
import time
import logging
from rich.logging import RichHandler
import typer
from parquet2json.converter import convert, Parquet2JSONError


CLI_CONTEXT_OPTIONS = {"help_option_names": ["-h", "--help"],
                       "ignore_unknown_options": True}

app = typer.Typer(add_completion=False,
                  no_args_is_help=True,
                  rich_markup_mode="rich",
                  context_settings=CLI_CONTEXT_OPTIONS)

logging.basicConfig(
    level="ERROR",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()]
)
log = logging.getLogger("rich")


@app.command("parquet2json",
             no_args_is_help=True,
             context_settings=CLI_CONTEXT_OPTIONS)
def parquet2json(
    parquet: str = typer.Argument(
        help="Input path/URI to parquet."),
    json: Path = typer.Argument(
        help="Output JSON path, or leave empty for STDOUT",
        default=None)
) -> None:
    """Convert parquet file to json."""
    start = time.time()
    try:
        convert(parquet_path=parquet, json_path=json)
        end = time.time()
        elapsed_time = end - start
        log.debug("Converted %s to %s in %.2f seconds.",
                  parquet,
                  json,
                  elapsed_time
                  )
    except Parquet2JSONError as e:
        log.error(e)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
