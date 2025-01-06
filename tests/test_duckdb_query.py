from datetime import date, datetime, timedelta
from collections import namedtuple

from hypothesis import given
from hypothesis import strategies as st
import duckdb
import pytest

from parquet_fe_prototype.duckdb_query import (
    PerspectiveFilters,
    FilterRule,
    perspective_to_duckdb,
)


@pytest.fixture(scope="session")
def con(rows):
    con = duckdb.connect(":memory:")
    con.execute("""
    CREATE TABLE numbers (
        integer_col INTEGER,
        float_col FLOAT,
        date_col DATE,
        datetime_col DATETIME,
        string_col VARCHAR,
        boolean_col BOOLEAN
    )""")
    for row in rows:
        con.execute(
            "INSERT INTO numbers VALUES ($integer_col, $float_col, $date_col, $datetime_col, $string_col, $boolean_col)",
            row._asdict(),
        )
    return con


BASE_OPERATORS = ["==", "!=", ">", ">=", "<", "<=", "is null", "is not null"]
NULL_OPERATORS = {"is null", "is not null"}


# Define type mappings and valid operators
TYPE_CONFIGS = {
    "boolean": {"value_strategy": st.booleans(), "operators": BASE_OPERATORS},
    "date": {
        "value_strategy": st.dates(min_value=date(1970, 1, 1)).map(
            lambda d: d.isoformat()
        ),
        "operators": BASE_OPERATORS,
    },
    "datetime": {
        "value_strategy": st.datetimes(min_value=datetime(1970, 1, 1)).map(
            lambda d: d.isoformat()
        ),
        "operators": BASE_OPERATORS,
    },
    "float": {"value_strategy": st.floats(), "operators": BASE_OPERATORS},
    "integer": {"value_strategy": st.integers(), "operators": BASE_OPERATORS},
    "string": {
        "value_strategy": st.text(min_size=0, max_size=10),
        "operators": BASE_OPERATORS
        + ["begins with", "contains", "ends with", "in", "not in"],
    },
}

col_type = st.sampled_from(list(TYPE_CONFIGS.keys()))
filter_tuple_strategy = st.shared(col_type, key="t").flatmap(
    lambda t: st.tuples(
        st.just(f"{t}_col"),
        st.sampled_from(TYPE_CONFIGS[t]["operators"]),
        TYPE_CONFIGS[t]["value_strategy"],
    )
)


def build_filter_rule(type, filter):
    if filter[1] in NULL_OPERATORS:
        filter = (filter[0], filter[1], "")
    return FilterRule(type=type, filter=filter)


filter_rule_strategy = st.builds(
    build_filter_rule, type=st.shared(col_type, key="t"), filter=filter_tuple_strategy
)
filter_rules_strategy = st.lists(filter_rule_strategy)
# get an op and a value from this


@given(filter_rules_strategy)
def test_duckdb_valid_query(con, rules):
    filters = PerspectiveFilters(
        table_name="numbers",
        filter_rules=rules,
    )
    query = perspective_to_duckdb(filters)
    try:
        results = con.execute(query.statement, query.values).fetchall()
    except Exception as e:
        print(query)
        print(rules)
        raise e
    assert isinstance(results, list)


@pytest.fixture(scope="session")
def rows():
    Row = namedtuple(
        "Row",
        [
            "integer_col",
            "float_col",
            "date_col",
            "datetime_col",
            "string_col",
            "boolean_col",
        ],
    )
    return [
        Row(
            integer_col=x,
            float_col=x + 0.5,
            date_col=date(2023, 12, 31) + timedelta(days=x),
            datetime_col=datetime(2023, 12, 31, 1, 1) + timedelta(days=x),
            string_col=str(x),
            boolean_col=x % 2 == 0,
        )
        for x in range(0, 5)
    ]


@pytest.mark.parametrize(
    "filter_rules,row_nums",
    [
        (
            [FilterRule(type="date", filter=("date_col", "==", "2024-01-01"))],
            [1],
        ),
        (
            [
                FilterRule(
                    type="date",
                    filter=("date_col", ">", "2024-01-02"),
                )
            ],
            [3, 4],
        ),
        (
            [
                FilterRule(
                    type="date",
                    filter=("date_col", ">", "2024-01-02"),
                ),
                FilterRule(type="float", filter=("float_col", "==", 3.5)),
            ],
            [3],
        ),
    ],
)
def test_spot_check_filters(
    con: duckdb.DuckDBPyConnection, filter_rules, rows, row_nums
):
    filters = PerspectiveFilters(
        table_name="numbers",
        filter_rules=filter_rules,
    )
    query = perspective_to_duckdb(filters)
    results = con.execute(query.statement, query.values).fetchall()
    assert len(results) == len(row_nums)
    assert results == [rows[i] for i in row_nums]
