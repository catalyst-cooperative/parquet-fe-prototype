import * as duckdb from '@duckdb/duckdb-wasm';
import * as arrow from 'apache-arrow';

const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

// Select a bundle based on browser checks
const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

const worker_url = URL.createObjectURL(
  new Blob([`importScripts("${bundle.mainWorker!}");`], {type: 'text/javascript'})
);

// Instantiate the asynchronus version of DuckDB-wasm
const worker = new Worker(worker_url);
const logger = new duckdb.ConsoleLogger();
const db = new duckdb.AsyncDuckDB(logger, worker);
await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
URL.revokeObjectURL(worker_url);

const baseUrl = "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/stable/";
const filename = "out_ferc714__summarized_demand.parquet";
const url = `${baseUrl}${filename}`;
console.log(url);
await db.registerFileURL(filename, url, duckdb.DuckDBDataProtocol.HTTP, false);


const c = await db.connect();

for await (const batch of await c.send(`
    SELECT * FROM ${filename} LIMIT 1000
`)) {
    console.log(batch);
} 
