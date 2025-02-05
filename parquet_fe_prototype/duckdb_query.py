"""Generate DuckDB queries."""

from dataclasses import dataclass
import itertools

from pydantic import BaseModel


def _camelize(string: str) -> str:
    """snake_case to camelCase."""
    words = string.split("_")
    return words[0] + "".join(word.capitalize() for word in words[1:])


class Filter(BaseModel):
    """Represent a filter.

    Some operations have two values (between) so we need two value slots."""

    field_name: str
    field_type: str
    operation: str
    value: str | int | float | bool | None = None
    value_to: str | int | float | bool | None = None

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


def __ag_filters_to_where(filters: list[Filter]) -> tuple[str, list]:
    """Convert FilterRules to a WHERE clause."""
    placeholder_casts = {"date": "?::DATE", "datetime": "epoch_ms(?::BIGINT)"}
    clause_templates = {
        "equals": "{col} = {placeholder}",
        "notequal": "{col} != {placeholder}",
        "greaterthan": "{col} > {placeholder}",
        "greaterthanorequal": "{col} >= {placeholder}",
        "lessthan": "{col} < {placeholder}",
        "lessthanorequal": "{col} <= {placeholder}",
        "inrange": "{col} BETWEEN {placeholder} AND {placeholder}",
        "contains": "{col} ILIKE CONCAT('%', {placeholder}, '%')",
        "notcontains": "{col} NOT ILIKE CONCAT('%', {placeholder}, '%')",
        "startswith": "STARTS_WITH({col}, {placeholder})",
        "endswith": "ENDS_WITH({col}, {placeholder})",
        "blank": "{col} IS NULL",
        "notblank": "{col} IS NOT NULL",
        "default": "{col} {op} {placeholder}",
    }

    where_clauses = ["true"]
    for filter in filters:
        placeholder = placeholder_casts.get(filter.field_type, "?")
        col = filter.field_name
        op = filter.operation.lower()

        clause_template = clause_templates.get(op, clause_templates["default"])
        where_clauses.append(
            clause_template.format(col=col, op=op, placeholder=placeholder)
        )

    possible_vals = itertools.chain.from_iterable(
        (f.value, f.value_to) for f in filters
    )
    vals = [v for v in possible_vals if v is not None]

    return " AND ".join(where_clauses), vals


def ag_grid_to_duckdb(name: str, filters: list[Filter]) -> QuerySpec:
    """Turn tabulator filters into a set of DuckDB queries for the frontend to run."""
    where, vals = __ag_filters_to_where(filters)
    query = f"SELECT * FROM {name} WHERE {where}"
    count_query = f"SELECT COUNT(*) FROM {name} WHERE {where} LIMIT 1"
    return QuerySpec(statement=query, count_statement=count_query, values=vals)
