import json

from flask import Flask, request, render_template


def create_app():
    app = Flask("parquet-fe-prototype", instance_relative_config=True)
    app.config.from_mapping(SECRET_KEY="dev")

    # TODO 2024-12-16: make real types
    # TODO 2024-12-16: pull this out of the PUDL package. Ideally we write out a datapackage.json...
    METADATA = {
        "out_eia923__boiler_fuel": {
            "description": "EIA-923 Monthly Boiler Fuel Consumption and Emissions, from EIA-923 Schedule 3.",
            "schema": {
                "boiler_id": {
                    "type": "string",
                    "description": "Alphanumeric boiler ID.",
                },
                "energy_source_code": {
                    "type": "string",
                    "description": "A 2-3 letter code indicating the energy source (e.g. fuel type) associated with the record.",
                },
                "report_date": {"type": "date", "description": "Date reported."},
                "fuel_consumed_mmbtu": {
                    "type": "number",
                    "description": "Total consumption of fuel in physical unit, year to date.",
                },
            },
        }
    }

    @app.get("/preview/<table>")
    def preview(table: str):
        return render_template("preview.html", table=table)

    def build_query_with_filters(table_name, filter, schema):
        query = f"SELECT * FROM {table_name} WHERE "

        where_clauses = ["true"]

        unary_ops = {"is null", "is not null"}

        op_conversion_templates = {
            "==": "{col} = {autoincrement_value}",
        }

        type_converters = {
            "date": "strptime(?,'%Y-%m-%d')",
            "number": "CAST(? AS DOUBLE)",
        }

        for col, op, val in filter:
            if val is None:
                continue
            autoincrement_value = type_converters.get(schema.get(col), "?")
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

    @app.get("/api/duckdb")
    def duckdb_query():
        base_url = "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/stable/"
        filename = f"{request.args.get("tableName")}.parquet"
        # actually return the filename and url also so that we can register that in duckdb
        filter_json = request.args.get("filter", "[]")
        schema_json = request.args.get("schema", "{}")
        # get the schema and use it to create filters
        filter = json.loads(filter_json)
        schema = json.loads(schema_json)
        query = build_query_with_filters(
            table_name=filename, filter=filter, schema=METADATA.get(request.args.get("tableName"))
        )
        for_download = json.loads(request.args.get("forDownload"))
        if not for_download:
            query += " LIMIT 1000"
        print(f"{query=}")
        return query

    return app
