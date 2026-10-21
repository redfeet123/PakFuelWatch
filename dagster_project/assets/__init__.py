"""Dagster assets package."""

from dagster_project.assets.bronze import (
    bronze_assets,
    bronze_brent,
    bronze_fx,
    bronze_hdx_food,
    bronze_worldbank,
)

__all__ = [
    "bronze_assets",
    "bronze_brent",
    "bronze_fx",
    "bronze_worldbank",
    "bronze_hdx_food",
]
