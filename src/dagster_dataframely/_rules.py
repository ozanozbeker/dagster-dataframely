"""One rule and everything this package derives about it, derived once.

A Dataframely rule is a name and an expression. Its check name, the column and rule name the `|` delimits, and the docstring behind it are this package's own derivations, and six places used to re-derive them from the string. `DescribedRule` holds them, so Dataframely's `|` is read here and nowhere else.

`validate_namespace` lives here rather than in `_naming` because it asks whether a schema's columns and its rules' check names collide, and both are properties of the records below. It walked the rules a second time before (ADR-0008).

Nothing is cached. ADR-0008 measured the guard and declined a `functools.cache` keyed on a class, which would hold a strong reference for the process lifetime. The records are built per call on the same terms.

**The expression stays lazy.** Dataframely builds a `@dy.rule()` body as `Rule(expr=lambda: ...)`, so reading `.expr` for every rule where the record is built costs a schema of forty rule bodies nine times what it costs to leave it alone. Nothing at definition time reads it: only a run's check metadata does.

Measured on dagster 1.13.20, dataframely 3.0.0, polars 1.44.1. Building the records costs about twice what reading Dataframely's rule dict alone costs, because every rule gets a `|` parse, a check name and a docstring lookup whether or not the caller wants them. Defining one asset went from 1.01 ms to 1.12 ms on a twelve-column schema, so a twenty-one asset code location pays about two milliseconds more per load. A `NamedTuple` constructs these 2.7 times faster and was declined: it forbids a leading underscore on a field, so `rule` would sit beside `expr` as a second route to the same object, and a six-field record would start answering to indexing and iteration.
"""

import inspect
from dataclasses import dataclass

import dataframely as dy
import polars as pl

# Neither has a public equivalent. `Rule` is what `_validation_rules` hands back and
# `RuleFactory` is where a `@dy.rule()`'s docstring survives. Covered by characterization
# tests (#16), and this is the one module that reaches for either.
from dataframely._rule import Rule, RuleFactory

from dagster_dataframely._naming import RESERVED_NAMESPACE, check_name
from dagster_dataframely.errors import CheckNameCollisionError, ReservedColumnError


@dataclass(frozen=True)
class DescribedRule:
    """One Dataframely validation rule, with everything this package says about it.

    The expression is reached through `expr`, not through the field holding it, so a caller never learns that a record wraps Dataframely's own `Rule`.

    Attributes
    ----------
    name
        The rule name Dataframely reports, `|`-delimited for column rules.
    check_name
        The asset-check name the `|` to `__` rewrite produces.
    column
        The column that owns the rule, or `None` for a rule no single column owns. A Python identifier cannot contain `|`, so its presence alone decides.
    rule_name
        The column-level rule name, which is Dataframely's own word for it: it builds a column rule's key as `f"{col_name}|{rule_name}"`, and `rule_name` is the column argument the rule was generated from. `None` wherever `column` is.
    description
        The rule's docstring, dedented, or `None` for a rule with no place to carry one. A `@dy.rule()` leaves its `RuleFactory` on the class, so the decorated function and its docstring stay reachable by name after the metaclass has built the `Rule`. Column rules are generated from column arguments and have no function to document.
    """

    name: str
    check_name: str
    column: str | None
    rule_name: str | None
    description: str | None
    # Dataframely's own rule, private because `expr` is the whole of what anything wants
    # from it and the laziness in the module docstring is the reason to keep the wrapper.
    _rule: Rule

    @property
    def expr(self) -> pl.Expr:
        """The rule's expression, resolved on the first read rather than where the record was built."""
        return self._rule.expr


def _described_rule(schema: type[dy.Schema], name: str, rule: Rule) -> DescribedRule:
    """Describe one rule.

    The docstring lookup is by name and needs no branch: a column rule's `|` makes `getattr` miss, and a `@dy.rule()`'s method name finds its factory.
    """
    column, delimiter, rule_name = name.partition("|")
    factory = getattr(schema, name, None)
    return DescribedRule(
        name=name,
        check_name=check_name(name),
        column=column if delimiter else None,
        rule_name=rule_name if delimiter else None,
        description=(
            inspect.getdoc(factory.validation_fn)
            if isinstance(factory, RuleFactory)
            else None
        ),
        _rule=rule,
    )


def described_rules(schema: type[dy.Schema]) -> dict[str, DescribedRule]:
    """Return the schema's validation rules, described, keyed by rule name.

    `with_cast=False` drops the `<column>|dtype` pseudo-rules. They would otherwise duplicate the column-schema check at a different severity and without blocking.

    Returns
    -------
    One record per rule, in the schema's own rule order. A caller wanting one rule indexes by the name Dataframely gives it; a caller wanting them all iterates the values.
    """
    return {
        name: _described_rule(schema, name, rule)
        for name, rule in schema._validation_rules(with_cast=False).items()  # noqa: SLF001
    }


def validate_namespace(schema: type[dy.Schema]) -> None:
    """Raise the two errors Dagster would otherwise report opaquely, or not at all.

    Raises
    ------
    ReservedColumnError
        A user column sits inside the reserved namespace.
    CheckNameCollisionError
        Two rules rewrite to the same asset-check name.
    """
    reserved: list[str] = [
        column for column in schema.columns() if column.startswith(RESERVED_NAMESPACE)
    ]
    if reserved:
        raise ReservedColumnError(schema.__name__, reserved)

    seen: dict[str, str] = {}
    for rule in described_rules(schema).values():
        if rule.check_name in seen:
            raise CheckNameCollisionError(
                schema.__name__, seen[rule.check_name], rule.name, rule.check_name
            )
        seen[rule.check_name] = rule.name
