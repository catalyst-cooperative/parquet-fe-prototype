# Parquet Frontend-only prototype

## Usage

Install `uv` and `npm`.

Install the required packages:

```
npm install
uv sync
```

Build the JS and CSS:
```
npm run build
```

Run the server:

```
uv run flask --app parquet_fe_prototype run --debug
```

and then go to `localhost:5000`.


## TODOs?

* make the filters -> duckdb converter actually work good
* fix rendering issue??
* tests
* types - for metadata, for filters
* tags
* update URL when search is searched
* add full-count row counter in preview
* make CSV export go faster
* add loading indicators
* remove export button in preview
* theming look good
* make header more useful - nav, FAQ, etc.
* add download as parquet button