# Parquet Frontend-only prototype

## Installation

Install `uv` and `npm`.

Install the required packages:

```
npm install
uv sync
```

## Running this thing

We have a docker compose file, but make sure to build the JS/CSS first:

```bash
$ npm run build
...
$ docker compose build && docker compose up
```

## Tests

We only have a few unit tests right now - no frontend testing or anything.

```
$ uv run pytest
```

## DB migration

## Deployment

1. run `make gcp-latest` to push the image up to GCP.
2. If necessary, run the Cloud Run job that runs a db migration.
2. re-deploy the service on Cloud Run.

## Architecture

We have a standard client-server-database situation going on.

For **search**:

1. The client sends search query to the server
2. The server queries against an in-memory search index. See the `/search` endpoint and the `search.py` file.
3. The server sends a list of matches back to the client

Via the magic of [`htmx`](https://www.htmx.org), if the search wasn't triggered by a whole page load, we only send back an HTML fragment.


For **preview**:

1. Client sends the filters that the user's applied to the server, and gets a DuckDB query back. See the `/api/duckdb` endpoint and `duckdb_query.py` files.
2. Client queries DuckDB (using [duckdb-wasm](https://duckdb.org/docs/api/wasm/overview.html)), which can read data from remote Parquet files.
3. The data comes back as Apache Arrow tables, which we put into the [Perspective](https://perspective.finos.org/) viewer.

The database is *only* used for storing users right now.