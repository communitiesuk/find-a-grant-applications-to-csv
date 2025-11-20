#!/usr/bin/env python3
"""Click-based CLI entrypoint for Find-a-Grant submissions to CSV."""

from pathlib import Path

import click

from find_a_grant_csv.cli import run_pipeline_sync


@click.command()
@click.argument("output_csv", required=False, type=click.Path(path_type=Path))
@click.option(
    "--api-base",
    required=True,
    help="Base URL (no trailing slash), e.g., 'https://api.example.gov.uk'",
)
@click.option(
    "--ggis-reference-number",
    required=True,
    help="GGIS reference number for the grant",
)
@click.option("--api-key", required=True, help="API key for the 'x-api-key' header")
def cli(
    output_csv: Path | None, api_base: str, ggis_reference_number: str, api_key: str
) -> None:
    """Fetch submissions, flatten to CSV, and write OUTPUT_CSV (or auto-name if omitted)."""
    out_path, num_apps, elapsed = run_pipeline_sync(
        api_base=api_base,
        ggis_reference_number=ggis_reference_number,
        api_key=api_key,
        output_csv=output_csv,
    )
    click.echo(f"Output written to: {out_path}")
    click.echo(f"Retrieved {num_apps} applications in {elapsed:.2f} seconds")


if __name__ == "__main__":
    cli()
