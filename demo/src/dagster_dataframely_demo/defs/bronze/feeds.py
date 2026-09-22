"""The bronze assets, which load a customer export and four order feeds unchanged."""

# demo: each asset returns sample rows from `_data`; a real pipeline reads its source here.

import dagster as dg
import polars as pl

from dagster_dataframely_demo import _data


@dg.asset
def raw_orders() -> pl.DataFrame:
    """Load the storefront's nightly order export."""
    return _data.storefront_orders()


@dg.asset
def raw_customers() -> pl.DataFrame:
    """Load the storefront's customer export."""
    return _data.storefront_customers()


@dg.asset
def raw_marketplace_orders() -> pl.DataFrame:
    """Load the marketplace's order feed."""
    return _data.marketplace_orders()


@dg.asset
def raw_partner_orders() -> pl.DataFrame:
    """Load the B2B partner's order export."""
    return _data.partner_orders()


@dg.asset
def raw_legacy_orders() -> pl.DataFrame:
    """Load the one-off export from the old order platform."""
    return _data.legacy_orders()
