"""`customers` validates the storefront's customer accounts."""

import dataframely as dy
import polars as pl

import dagster_dataframely as dd


class CustomersSchema(dy.Schema):
    """A storefront customer account."""

    customer_id = dy.String(primary_key=True, description="Account identifier.")
    email = dy.String(nullable=False, description="Address for receipts.")
    lifetime_value = dy.Float64(nullable=False, min=0.0, description="Spend to date.")


@dd.asset(CustomersSchema)
def customers(raw_customers: pl.DataFrame) -> pl.DataFrame:
    """Validate the storefront's customer accounts."""
    return raw_customers
