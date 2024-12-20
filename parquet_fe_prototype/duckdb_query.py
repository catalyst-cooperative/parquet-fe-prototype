"""Generate DuckDB queries."""

from pydantic import BaseModel

def _camelize(string: str) -> str:
    words = string.split('_')
    return words[0] + ''.join(word.capitalize() for word in words[1:])


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


def perspective_to_duckdb(perspective_filters: PerspectiveFilters):
    table_name = perspective_filters.table_name
    filter_rules = perspective_filters.filter_rules
    query = f"SELECT * FROM {table_name} WHERE "

    where_clauses = ["true"]

    unary_ops = {"is null", "is not null"}

    op_conversion_templates = {
        "==": "{col} = {autoincrement_value}",
        "begins with": "STARTS_WITH({col}, {autoincrement_value})",
        "contains": "CONTAINS({col}, {autoincrement_value})",
        "ends with": "ENDS_WITH({col}, {autoincrement_value})",
    }

    type_converters = {
        "date": "?::DATE",
        "datetime": "?::TIMESTAMP",
        "number": "?::DOUBLE",
    }

    for filter_rule in filter_rules:
        col, op, val = filter_rule.filter
        col_type = filter_rule.type
        if val is None:
            continue
        autoincrement_value = type_converters.get(col_type, "?")
        if op in op_conversion_templates:
            where_clauses.append(
                op_conversion_templates[op].format(
                    col=col, op=op, autoincrement_value=autoincrement_value
                )
            )
        elif op.lower() in unary_ops:
            where_clauses.append(f"{col} {op}")
        else:
            where_clauses.append(f"{col} {op} {autoincrement_value}")

    query += " AND ".join(where_clauses)
    return query