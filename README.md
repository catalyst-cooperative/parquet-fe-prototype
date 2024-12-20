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

and then go to `localhost:5000/search`.


## TODOs?

* tests
* make the filters -> duckdb converter actually work good
* add full-count row counter in preview
* autosuggest
* types - for filters
* tags
* make CSV export go faster
* remove export button in preview
* make header more useful - nav, FAQ, etc.
