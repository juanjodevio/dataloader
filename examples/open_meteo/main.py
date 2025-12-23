"""Run Open Meteo examples.

Provides two recipes:
- api_to_local.yml  (API -> local filestore JSONL)
- api_to_duckdb.yml (API -> DuckDB file)
"""

from __future__ import annotations

import argparse
from pathlib import Path

from dataloader import run_recipe_from_yaml


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Open Meteo recipes")
    parser.add_argument(
        "--recipe",
        choices=["api_to_local", "api_to_duckdb"],
        default="api_to_duckdb",
        help="Recipe to run",
    )
    args = parser.parse_args()

    recipe_file = f"{args.recipe}.yml"
    recipe_path = Path(__file__).parent / "recipes" / recipe_file
    run_recipe_from_yaml(str(recipe_path), state_dir=".state")


if __name__ == "__main__":
    main()
