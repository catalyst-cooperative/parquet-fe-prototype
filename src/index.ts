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
  filter: [string, string, string];
  type: string;
}

globalThis.initializePreview = initializePreview;

let duckDBInitialized = false;
const db = await initializeDuckDB();
const c = await db.connect();
globalThis.c = c;
const perspectiveWorker = await perspective.worker();
const viewer = document.getElementsByTagName("perspective-viewer")[0];

let table;
let tableName: string;
let timeout: number;

viewer.addEventListener("perspective-config-update", reapplyFilters);

const downloader = document.getElementById("csv-download") as HTMLButtonElement;
downloader.onclick = downloadAsCsv;


async function initializeDuckDB(): Promise<duckdb.AsyncDuckDB> {
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
  duckDBInitialized = true;
  return db;
}

async function addTableToDuckDB(db: duckdb.AsyncDuckDB, tableName: string) {
  const baseUrl = "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/"
  const filename = `${tableName}.parquet`;
  const url = `${baseUrl}${filename}`;
  await db.registerFileURL(filename, url, duckdb.DuckDBDataProtocol.HTTP, false);
}

async function getDuckDBQuery(
  { tableName, filter_rules: filter_rules, forDownload = false }
    : { tableName: string, filter_rules: Array<FilterRule>, forDownload?: boolean }
): Promise<{ statement: string, count_statement: string, values: Array<any> }> {
  const params = new URLSearchParams(
    { perspective_filters: JSON.stringify({ tableName: `${tableName}.parquet`, filter_rules }), forDownload: JSON.stringify(forDownload) }
  );
  const resp = await fetch("/api/duckdb?" + params);
  const query = await resp.json();
  console.log("query", query);
  return query
}

async function getInitialTableData(
  tableName: string, c: duckdb.AsyncDuckDBConnection
): Promise<Array<arrow.Table>> {
  const { statement, count_statement: countStatement } = await getDuckDBQuery({ tableName, filter_rules: [], forDownload: false });
  return await Promise.all([c.query(statement), c.query(countStatement)]);
}


async function resetCounters() {
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
async function updateCounters(displayedRowCount: number, matchingRowCount: number) {
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
  const { filter: rawFilter } = await viewer.save();
  const filter = rawFilter.filter((e: Array<string>) => e[2] !== null);
  const schema = await table.schema();
  const filterRules = filter.map(
    ([col, op, val]: [string, string, string]) => {
      return { "filter": [col, op, val], "type": schema[col] };
    }
  );

  console.log("filter rules ", filterRules);
  const { statement, count_statement: countStatement, values: filterVals } = await getDuckDBQuery({ tableName, filter_rules: filterRules, forDownload: forDownload });
  const stmt = await c.prepare(statement);
  const counter = await c.prepare(countStatement);
  const [countResult, newData] = await Promise.all(
    [counter.query(...filterVals), stmt.query(...filterVals)]
  );
  const matchingRowCount = countResult?.getChild("count_star()")?.get(0);
  updateCounters(newData.numRows, matchingRowCount);
  return newData;
}

async function initializePreview(name: string) {
  tableName = name;
  resetCounters();
  globalThis.pq = `${tableName}.parquet`;
  document.getElementsByClassName("preview-panel")[0].style.display = "block";
  document.getElementById("table-name").innerHTML = "loading...";

  const downloader = document.getElementById("csv-download") as HTMLButtonElement;
  downloader.disabled = true;
  if (!duckDBInitialized) {
    window.setTimeout(() => initializePreview(name), 500);
  }
  await addTableToDuckDB(db, tableName);
  const viewer = document.getElementsByTagName("perspective-viewer")[0];
  const [tableData, countResult] = await getInitialTableData(tableName, c);
  const matchingRowCount = countResult?.getChild("count_star()")?.get(0);
  await updateCounters(tableData.numRows, matchingRowCount);
  document.getElementById("table-name").innerHTML = tableName;
  downloader.disabled = false;
  table = await perspectiveWorker.table(arrow.tableToIPC(tableData, "file"));
  await viewer.load(table);
  viewer.restore({ settings: true });
}


async function reapplyFilters() {
  window.clearTimeout(timeout);
  const debounceMs = 300;
  timeout = window.setTimeout(async () => {
    const newData = await _getTableDataForViewer(tableName, viewer, c);
    console.log("got table data for viewer");
    globalThis.data = newData;
    table.replace(arrow.tableToIPC(newData, "file"));
  }, debounceMs);
};

async function downloadAsCsv() {
  const newData = await _getTableDataForViewer(tableName, viewer, c, true);
  // TODO 2024-12-16: make this go faster or non-blocking. worker-ify it probably? but maybe just async it.
  const blob = arrowToCsv(newData);
  downloadBlob(blob);
}

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