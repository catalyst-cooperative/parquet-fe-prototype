"""Generate DuckDB queries."""

from dataclasses import dataclass
from datetime import datetime, timezone

from pydantic import BaseModel


def _camelize(string: str) -> str:
    """snake_case to camelCase."""
    words = string.split("_")
    return words[0] + "".join(word.capitalize() for word in words[1:])


class FilterRule(BaseModel):
    """To successfully translate a filter into DuckDB SQL, we need to know the
    type of the field as well as the actual field name / operation /
    contents."""

    type: str
    # TODO 2025-01-15 probably turn this into name/op/value fields instead of a
    # list that relies on position; this will involve translating the list that
    # Perspective spits out into this structure.
    filter: tuple[str, str, str | int | float | bool]

    class Config:
        alias_generator = _camelize
        populate_by_name = True


class PerspectiveFilters(BaseModel):
    """What you need from Perspective to generate a DuckDB query."""

    table_name: str
    filter_rules: list[FilterRule]

    class Config:
        alias_generator = _camelize
        populate_by_name = True


@dataclass
class QuerySpec:
    """Description of a query we should execute on the frontend. Includes a
    separate statement to just get the counts."""

    statement: str
    count_statement: str
    values: list


def _filter_rules_to_where(filter_rules: list[FilterRule]) -> tuple[str, list]:
    """Convert FilterRules to a WHERE clause."""
    placeholder_casts = {"date": "?::DATE", "datetime": "?::TIMESTAMP"}
    clause_templates = {
        "==": "{col} = {placeholder}",
        "begins with": "STARTS_WITH({col}, {placeholder})",
        "contains": "CONTAINS({col}, {placeholder})",
        "ends with": "ENDS_WITH({col}, {placeholder})",
        "is null": "{col} {op}",
        "is not null": "{col} {op}",
        "default": "{col} {op} {placeholder}",
    }

    # ignore `foo == null` filters - when you first drag a column into the
    # 'where' section, that is what automatically applies. would often cause
    # spurious empty return sets.
    filters_to_apply = [f for f in filter_rules if f.filter[1:] != ("==", None)]

    where_clauses = ["true"]
    for filter_rule in filters_to_apply:
        placeholder = placeholder_casts.get(filter_rule.type, "?")
        col, op, _ = filter_rule.filter
        clause_template = clause_templates.get(op, clause_templates["default"])
        where_clauses.append(
            clause_template.format(col=col, op=op, placeholder=placeholder)
        )

    vals = [
        f.filter[2]
        for f in filters_to_apply
        if f.filter[1] not in {"is null", "is not null"}
    ]

    return " AND ".join(where_clauses), vals


def perspective_to_duckdb(perspective_filters: PerspectiveFilters) -> QuerySpec:
    """Turn perspective filters into a set of DuckDB queries for the frontend to run."""
    where, vals = _filter_rules_to_where(perspective_filters.filter_rules)
    table_name = perspective_filters.table_name
    query = f"SELECT * FROM {table_name} WHERE {where}"
    count_query = f"SELECT COUNT(*) FROM {table_name} WHERE {where} LIMIT 1"
    return QuerySpec(statement=query, count_statement=count_query, values=vals)
