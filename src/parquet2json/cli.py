"""CLI entry point for the application."""
from pathlib import Path
import time
import rich
import typer
from parquet2json.converter import convert, Parquet2JSONError


CLI_CONTEXT_OPTIONS = {"help_option_names": ["-h", "--help"],
                       "ignore_unknown_options": True}

app = typer.Typer(add_completion=False,
                  no_args_is_help=True,
                  rich_markup_mode="rich",
                  context_settings=CLI_CONTEXT_OPTIONS)


@app.command("parquet2json",
             no_args_is_help=True,
             context_settings=CLI_CONTEXT_OPTIONS)
def parquet2json(
    parquet: str = typer.Argument(
        help="Input path to a parquet."),
    json: Path = typer.Argument(
        help="Output path to a JSON.")
) -> None:
    """Convert parquet file to json."""
    start = time.time()
    try:
        convert(parquet_in=parquet, json_out=json)
        end = time.time()
        elapsed_time = end - start
        rich.print((f"Converted {parquet} to {json}"),
                   (f"in {elapsed_time:.2f} seconds."))
    except Parquet2JSONError as e:
        rich.print(f"[red]Exiting...The following error occurred.[/red]\n{e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
