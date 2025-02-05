from datetime import date, datetime, timedelta
from collections import namedtuple

import duckdb
import pytest

from parquet_fe_prototype.duckdb_query import Filter, ag_grid_to_duckdb


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
    "filters,row_nums",
    [
        (
            [
                Filter(
                    field_name="date_col",
                    field_type="date",
                    operation="equals",
                    value="2024-01-01",
                )
            ],
            [1],
        ),
        (
            [
                Filter(
                    field_name="date_col",
                    field_type="date",
                    operation="greaterThan",
                    value="2024-01-02",
                )
            ],
            [3, 4],
        ),
        (
            [
                Filter(
                    field_name="date_col",
                    field_type="date",
                    operation="greaterThan",
                    value="2024-01-02",
                ),
                Filter(
                    field_type="float",
                    field_name="float_col",
                    operation="equals",
                    value=3.5,
                ),
            ],
            [3],
        ),
        (
            [
                Filter(
                    field_name="date_col",
                    field_type="date",
                    operation="inRange",
                    value="2024-01-02",
                    valueTo="2024-01-04",
                )
            ],
            [2, 3, 4],
        ),
    ],
)
def test_spot_check_filters(con: duckdb.DuckDBPyConnection, filters, rows, row_nums):
    query = ag_grid_to_duckdb("numbers", filters)
    results = con.execute(query.statement, query.values).fetchall()
    assert len(results) == len(row_nums)
    assert results == [rows[i] for i in row_nums]
