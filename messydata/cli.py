import json
import sys

import click

from .schema import DatasetSchema


@click.group()
def main():
    """MessyData — synthetic dirty data generator."""


@main.command()
@click.argument("config", type=click.Path(exists=True))
@click.option("--rows", "-n", default=1000, show_default=True, help="Number of rows to generate.")
@click.option("--seed", "-s", default=42, show_default=True, help="Random seed.")
@click.option("--output", "-o", default=None, help="Output file path. Defaults to stdout (CSV).")
@click.option(
    "--format", "-f", "fmt",
    type=click.Choice(["csv", "parquet", "json", "jsonl"]),
    default=None,
    help="Output format. Inferred from --output extension if not set; defaults to csv.",
)
def generate(config, rows, seed, output, fmt):
    """Generate a dataset from CONFIG and write it to a file or stdout."""
    from .pipeline import Pipeline

    # Infer format from output extension if not explicitly set
    if fmt is None:
        if output is None:
            fmt = "csv"
        elif output.endswith(".parquet"):
            fmt = "parquet"
        elif output.endswith(".json"):
            fmt = "json"
        elif output.endswith(".jsonl"):
            fmt = "jsonl"
        else:
            fmt = "csv"

    if fmt == "parquet" and output is None:
        raise click.UsageError("parquet format requires --output (cannot write to stdout).")

    df = Pipeline.from_config(config).run(n_rows=rows, seed=seed)

    if fmt == "csv":
        text = df.to_csv(index=False)
        if output:
            with open(output, "w") as f:
                f.write(text)
        else:
            click.echo(text, nl=False)

    elif fmt == "json":
        text = df.to_json(orient="records", indent=2)
        if output:
            with open(output, "w") as f:
                f.write(text)
        else:
            click.echo(text)

    elif fmt == "jsonl":
        text = df.to_json(orient="records", lines=True)
        if output:
            with open(output, "w") as f:
                f.write(text)
        else:
            click.echo(text, nl=False)

    elif fmt == "parquet":
        df.to_parquet(output, index=False)

    if output:
        click.echo(f"Wrote {len(df)} rows to {output}", err=True)


@main.command()
@click.argument("config", type=click.Path(exists=True))
def validate(config):
    """Validate CONFIG and report any errors."""
    try:
        schema = DatasetSchema.from_yaml(config)
        click.echo(
            f"✓ Valid — {len(schema.fields)} fields, {len(schema.anomalies)} anomalies",
            err=False,
        )
        sys.exit(0)
    except Exception as e:
        click.echo(f"✗ Invalid: {e}", err=True)
        sys.exit(1)


@main.command()
def schema():
    """Print the JSON schema for the MessyData config format."""
    from .schema import DatasetSchema

    click.echo(json.dumps(DatasetSchema.model_json_schema(), indent=2))
