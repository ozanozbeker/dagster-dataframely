"""The reserved `dy_` namespace and the names built in it.

`dy_` is not a setting, so check names are the same in every project.
"""

# Names repeat `dy_` literally, so grep finds every use.
RESERVED_NAMESPACE = "dy_"

COLUMN_SCHEMA_CHECK = "dy_schema__columns"


def get_check_name_for_rule(rule_name: str) -> str:
    """Return the asset check name for the Dataframely rule named `rule_name`.

    The error rows' rule columns and the checks at `rule` granularity have this name.

    Returns
    -------
    `dy_rule__` followed by `rule_name` with `|` replaced by `__`: `amount|min` becomes `dy_rule__amount__min`.
    """
    return f"dy_rule__{rule_name.replace('|', '__')}"


def collection_filter_check_name(filter_name: str) -> str:
    """Return the asset check name for the collection filter named `filter_name`.

    Not `dy_rule__`, because Dataframely allows a collection filter `amount__min` beside a member rule `amount|min`, and both would become `dy_rule__amount__min`.
    """
    return f"dy_filter__{filter_name}"


def column_check_name(column: str) -> str:
    """Return the check name for a column's rules."""
    return f"dy_col__{column}"
