"""Generate DuckDB queries."""

from dataclasses import dataclass
from datetime import datetime, timezone

from pydantic import BaseModel


def _camelize(string: str) -> str:
    words = string.split("_")
    return words[0] + "".join(word.capitalize() for word in words[1:])


class FilterRule(BaseModel):
    type: str
    filter: tuple[str, str, str | int | float | bool]

    class Config:
        alias_generator = _camelize
        populate_by_name = True


class PerspectiveFilters(BaseModel):
    table_name: str
    filter_rules: list[FilterRule]

    class Config:
        alias_generator = _camelize
        populate_by_name = True


@dataclass
class QuerySpec:
    """Description of a query we should execute on the frontend."""

    statement: str
    values: list


def perspective_to_duckdb(perspective_filters: PerspectiveFilters) -> QuerySpec:
    filter_rules = perspective_filters.filter_rules
    where_clauses = ["true"]

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

    vals = [
        f.filter[2]
        for f in filters_to_apply
        if f.filter[1] not in {"is null", "is not null"}
    ]

    for filter_rule in filters_to_apply:
        placeholder = placeholder_casts.get(filter_rule.type, "?")
        col, op, _ = filter_rule.filter
        clause_template = clause_templates.get(op, clause_templates["default"])
        where_clauses.append(
            clause_template.format(col=col, op=op, placeholder=placeholder)
        )

    table_name = perspective_filters.table_name
    query = f"SELECT * FROM {table_name} WHERE "
    query += " AND ".join(where_clauses)
    return QuerySpec(statement=query, values=vals)
