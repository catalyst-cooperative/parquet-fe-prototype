# Parquet Frontend-only prototype

## Usage
```
npm install
npm run start
```

and then go to `localhost:8080`.


## Notes

How we could make unlimited CSV downloads:

1. load a parquet file as a table using duckdb. only the first e.g. 1k lines.
2. shove that into perspective
3. onclick handler for perspective:
  1. convert config into a new duckdb query. only for the *filters*
  2. run the new query (with limit 1K or something), if the query is different from the old query
  3. run table.update()
4. if we click a *download raw data as CSV* button:
  0. use viewer.save() to get the json config.
  1. turn that json config into a duckdb query. ignore expressions, ignore pivots too.
  2. hit that with NO LIMIT, convert to csv, make file download 


There will still be the built in "export as CSV/json" buttons... we can get rid of those via CSS to avoid confusing users.
