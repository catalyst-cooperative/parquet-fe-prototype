import * as duckdb from '@duckdb/duckdb-wasm';
import * as arrow from 'apache-arrow';
import perspective, { Table } from "@finos/perspective";
import "@finos/perspective-viewer";
import "@finos/perspective-viewer-datagrid";
import "@finos/perspective-viewer-d3fc";

import "@finos/perspective-viewer/dist/css/pro-dark.css";
import "./index.css";
import { PerspectiveViewerElement } from '@finos/perspective-viewer/dist/pkg/perspective-viewer';

interface FilterRule {
  /**
   * Describe one column filter. Mirrors FilterRule in the Python code.
   * 
   * TODO 2025-01-15: Define these interfaces in one place - maybe with JSONSchema?
   *
   * TODO 2025-01-15: turn the array of [string, string, string] into separate
   * field name/operator/value fields.
   */
  filter: [string, string, string];
  type: string;
}

// This needs to be in the global namespace so that any new preview buttons returned from the search can have access to this function.
globalThis.initializePreview = initializePreview;

// TODO 2025-01-15: There's a bunch of global variables here to manage shared
// state. Probably worth using a lightweight framework like Svelte at this point.
let DUCK_DB_INITIALIZED = false;
const DB = await _initializeDuckDB();
const CONN = await DB.connect();
const PERSPECTIVE_WORKER = await perspective.worker();
const VIEWER = document.getElementsByTagName("perspective-viewer")[0];

let TABLE: Table;
let TABLE_NAME: string;
let FILTER_REAPPLICATION_DEBOUNCE: number;

VIEWER.addEventListener("perspective-config-update", () => reapplyFilters(VIEWER));

const downloader = document.getElementById("csv-download") as HTMLButtonElement;
downloader.onclick = downloadAsCsv;


async function initializePreview(name: string) {
  /**
   * The entry-point from the big green button.
   * 
   * Makes sure everything is initialized properly, then loads the data.
   */
  TABLE_NAME = name;
  _hideExcessUi(VIEWER);
  _resetCounters();
  // TODO 2025-01-15 use the 'is-hidden' class to manage visibility instead -
  // then we can also use that class to make fancy animations.
  document.getElementsByClassName("preview-panel")[0].style.display = "block";
  // TODO 2025-01-15 use the bulma skeleton blocks to show loading-ness
  document.getElementById("table-name").innerHTML = "loading...";

  const downloader = document.getElementById("csv-download") as HTMLButtonElement;
  downloader.disabled = true;
  if (!DUCK_DB_INITIALIZED) {
    // try again soon if DuckDB hasn't quite been initialized yet.
    window.setTimeout(() => initializePreview(name), 500);
  }
  await _addTableToDuckDB(DB, TABLE_NAME);
  document.getElementById("table-name").innerHTML = TABLE_NAME;
  downloader.disabled = false;
  reapplyFilters(VIEWER);
}


async function reapplyFilters(viewer) {
  /**
   * Re-get data from DuckDB based on Perspective viewer state.
   */
  window.clearTimeout(FILTER_REAPPLICATION_DEBOUNCE);
  const debounceMs = 300;
  FILTER_REAPPLICATION_DEBOUNCE = window.setTimeout(async () => {
    const newData = await _getTableDataForViewer(TABLE_NAME, viewer, CONN);
    console.log("got table data for viewer");
    if (TABLE === undefined) {
      TABLE = await PERSPECTIVE_WORKER.table(arrow.tableToIPC(newData, "file"));
      await viewer.load(TABLE);
      viewer.restore({ settings: true });
    }
    TABLE.replace(arrow.tableToIPC(newData, "file"));
  }, debounceMs);
};

async function downloadAsCsv() {
  /**
   * Download the FILTERED but NON-LIMITED data from Parquet and turn it into a CSV.
   */
  function arrowToCsv(table: arrow.Table): Blob {
    const csv = table.toArray()
      .map(row => Object.values(row).join(','))
      .join('\n');

    const headers = table.schema.fields.map(f => f.name).join(',');
    const csvWithHeaders = headers + '\n' + csv;

    return new Blob([csvWithHeaders], { type: 'text/csv' });
  }

  function downloadBlob(blob: Blob) {
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = "mydata.csv";
    link.click();
    URL.revokeObjectURL(url);
  }

  const newData = await _getTableDataForViewer(TABLE_NAME, VIEWER, CONN, true);
  // TODO 2024-12-16: make this go faster or non-blocking. worker-ify it probably? but maybe just async it.
  //
  const blob = arrowToCsv(newData);
  downloadBlob(blob);
}


function _hideExcessUi(viewer) {
  /**
   * Reach into the shadow DOM and hide the menu bar & the "new column" button.
   *
   * It doesn't feel nice to hide the "export" button, but we're replacing it with a * *better* one so that seems OK.
   */
  const sheet = new CSSStyleSheet();
  sheet.replaceSync("#menu-bar, #add-expression { display: none !important; }");
  viewer.shadowRoot.adoptedStyleSheets.push(sheet);
}

async function _initializeDuckDB(): Promise<duckdb.AsyncDuckDB> {
  /**
   * Get the duckdb library that works best for your system, then spin up a web
   * worker for it.
   */
  const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

  // Select a bundle based on browser checks
  const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

  const worker_url = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker!}");`], { type: 'text/javascript' })
  );

  // Instantiate the asynchronous version of DuckDB-wasm
  const worker = new Worker(worker_url);
  const logger = new duckdb.ConsoleLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(worker_url);
  DUCK_DB_INITIALIZED = true;
  return db;
}


async function _addTableToDuckDB(db: duckdb.AsyncDuckDB, tableName: string) {
  /**
   * Register the table in DuckDB so that it can cache useful metadata etc.
   */
  const baseUrl = "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/"
  const filename = `${tableName}.parquet`;
  const url = `${baseUrl}${filename}`;
  await db.registerFileURL(filename, url, duckdb.DuckDBDataProtocol.HTTP, false);
}


// TODO 2025-01-15: the input type to this function mirrors PerspectiveFilters in Python.
async function _getDuckDBQuery(
  { tableName, filter_rules: filter_rules, forDownload = false }
    : { tableName: string, filter_rules: Array<FilterRule>, forDownload?: boolean }
): Promise<{ statement: string, count_statement: string, values: Array<any> }> {
  /**
   * Get DuckDB query from the backend, based on the filter rules & what table we're looking at.
   */
  const params = new URLSearchParams(
    { perspective_filters: JSON.stringify({ tableName: `${tableName}.parquet`, filter_rules }), forDownload: JSON.stringify(forDownload) }
  );
  const resp = await fetch("/api/duckdb?" + params);
  const query = await resp.json();
  console.log("query", query);
  return query
}


async function _resetCounters() {
  const displayedRows = document.getElementById("displayed-rows");
  const matchingRows = document.getElementById("matching-rows");
  if (displayedRows !== null) {
    displayedRows.innerText = "???";
    displayedRows.className = "has-text-weight-bold";
  }
  if (matchingRows !== null) {
    matchingRows.innerText = "???";
    matchingRows.className = "has-text-weight-bold";
  }
}


async function _updateCounters(displayedRowCount: number, matchingRowCount: number) {
  const displayedRows = document.getElementById("displayed-rows");
  const matchingRows = document.getElementById("matching-rows");
  console.log(`Got ${displayedRowCount}/${matchingRowCount} rows`);
  const isIncompletePreview = matchingRowCount > displayedRowCount;
  if (displayedRows !== null) {
    displayedRows.innerText = `${displayedRowCount}`;
    displayedRows.className = isIncompletePreview ? "has-text-weight-bold has-text-warning" : "has-text-weight-bold";
  }
  if (matchingRows !== null) {
    matchingRows.innerText = `${matchingRowCount}`;
    matchingRows.className = isIncompletePreview ? "has-text-weight-bold has-text-warning" : "has-text-weight-bold";
  }
}


async function _getTableDataForViewer(
  tableName: string, viewer: PerspectiveViewerElement, c: duckdb.AsyncDuckDBConnection, forDownload: boolean = false
): Promise<arrow.Table> {
  /**
   * Get the dang data:
   * 
   * 1. get filter state from Perspective
   * 2. get DuckDB query from server, based on filter state
   * 3. run both the "get a sample of data" and the "count data rows" queries in DuckDB
   * 4. update the row counters
   */
  // Turn filters from Perspective state into the shape Python server expects
  const { filter: rawFilter } = await viewer.save();
  const filter = rawFilter.filter((e: Array<string>) => e[2] !== null);
  let filterRules: Array<FilterRule> = [];
  if (filter.length > 0) {
    const schema = await TABLE.schema();
    filterRules = filter.map(
      ([col, op, val]: [string, string, string]) => {
        if (schema[col] == "datetime") {

        }
        return { "filter": [col, op, val], "type": schema[col] };
      }
    );
  }

  // TODO 2025-01-15 this shape mirrors python QuerySpec, should define that somewhere too.
  const { statement, count_statement: countStatement, values: filterVals } = await _getDuckDBQuery(
    { tableName, filter_rules: filterRules, forDownload: forDownload }
  );
  const stmt = await c.prepare(statement);
  const counter = await c.prepare(countStatement);
  const [countResult, newData] = await Promise.all(
    [counter.query(...filterVals), stmt.query(...filterVals)]
  );
  const matchingRowCount = countResult?.getChild("count_star()")?.get(0);
  _updateCounters(newData.numRows, matchingRowCount);
  return newData;
}