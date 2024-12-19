import * as duckdb from '@duckdb/duckdb-wasm';
import * as arrow from 'apache-arrow';
import perspective, { Table } from "@finos/perspective";
import "@finos/perspective-viewer";
import "@finos/perspective-viewer-datagrid";
import "@finos/perspective-viewer-d3fc";

import "@finos/perspective-viewer/dist/css/pro-dark.css";
import "@finos/perspective-viewer/dist/css/pro.css";
import "./index.css";
import { PerspectiveViewerElement } from '@finos/perspective-viewer/dist/pkg/perspective-viewer';

interface FilterRule {
  filter: [string, string, string];
  type: string;
}

globalThis.initializePreview = initializePreview;

const db = await initializeDuckDB();
const c = await db.connect();
const perspectiveWorker = await perspective.worker();
const viewer = document.getElementsByTagName("perspective-viewer")[0];

let table;
let tableName: string;
let timeout: number;

const darkModeQuery = window.matchMedia('(prefers-color-scheme: dark)');
viewer.addEventListener("perspective-config-update", reapplyFilters);

const downloader = document.getElementById("csv-download") as HTMLButtonElement;
downloader.onclick = downloadAsCsv;

darkModeQuery.addEventListener('change', switchTheme);

function switchTheme(query) {
  const theme = query.matches ? 'Pro Dark' : 'Pro';
  console.log("switching theme to", theme)
  viewer.restore({ theme });
}

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
  return db;
}

async function addTableToDuckDB(db: duckdb.AsyncDuckDB, tableName: string) {
  const baseUrl = "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/stable/"
  const filename = `${tableName}.parquet`;
  const url = `${baseUrl}${filename}`;
  await db.registerFileURL(filename, url, duckdb.DuckDBDataProtocol.HTTP, false);
}

async function getDuckDBQuery(
  { tableName, filter, forDownload = false }
    : { tableName: string, filter: Array<FilterRule>, forDownload?: boolean }
) {
  const params = new URLSearchParams(
    { tableName, filter: JSON.stringify(filter), forDownload: JSON.stringify(forDownload) }
  );
  const resp = await fetch("/api/duckdb?" + params);
  const query = await resp.text();
  return query
}

async function getInitialTableData(
  tableName: string, c: duckdb.AsyncDuckDBConnection
): Promise<arrow.Table> {
  const query = await getDuckDBQuery({ tableName, filter: [], forDownload: false });
  return await c.query(query);
}


async function _getTableDataForViewer(
  tableName: string, viewer: PerspectiveViewerElement, c: duckdb.AsyncDuckDBConnection, forDownload: boolean = false
): Promise<arrow.Table> {
  const { filter: rawFilter } = await viewer.save();
  const filter = rawFilter.filter((e: Array<string>) => e[2] !== null);
  console.log("filter ", filter);
  const schema = await table.schema();
  const filterRules = filter.map(
    ([col, op, val]: [string, string, string]) => {
      return { "filter": [col, op, val], "type": schema[col] };
    }
  );

  const filterVals = filter.map((e: Array<string>) => e[2]);
  const query = await getDuckDBQuery({ tableName, filter: filterRules, forDownload: forDownload });
  const stmt = await c.prepare(query);
  console.log("query ", query);
  console.log("filtervals ", filterVals);
  const newData = await stmt.query(...filterVals);
  console.log(`got ${newData.numRows} rows of data`);
  return newData;
}

async function initializePreview(name: string) {
  tableName = name;
  document.getElementsByClassName("preview-panel")[0].style.display = "block";
  document.getElementById("table-name").innerHTML = "loading...";
  downloader.disabled = true;
  await addTableToDuckDB(db, tableName);
  const viewer = document.getElementsByTagName("perspective-viewer")[0];
  const tableData = await getInitialTableData(tableName, c);
  document.getElementById("table-name").innerHTML = tableName;
  downloader.disabled = false;
  table = await perspectiveWorker.table(arrow.tableToIPC(tableData, "file"));
  await viewer.load(table);
  switchTheme(darkModeQuery);
}


async function reapplyFilters() {
  window.clearTimeout(timeout);
  const debounceMs = 300;
  timeout = window.setTimeout(async () => {
    const newData = await _getTableDataForViewer(tableName, viewer, c);
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