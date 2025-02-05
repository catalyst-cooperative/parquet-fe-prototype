import * as duckdb from '@duckdb/duckdb-wasm';
import * as arrow from 'apache-arrow';

import { DATE_TS_TYPE_IDS, DATE_TYPE_IDS, TIMESTAMP_TYPE_IDS } from './constants';
import { createGrid, ModuleRegistry, AllCommunityModule, GridApi, GridOptions } from 'ag-grid-community';
import Alpine, { AlpineComponent } from 'alpinejs';

import "./index.css";

ModuleRegistry.registerModules([AllCommunityModule]);

interface Filter {
  /**
   * Describe one column filter. Mirrors FilterRule in the Python code.
   * 
   * TODO 2025-01-15: Define these interfaces in one place - maybe with JSONSchema?
   */
  fieldName: string;
  fieldType: string;
  operation: string;
  value: any;
  valueTo: any;
}

interface QuerySpec {
  /**
   * What we need to send a query to duckdb.
   */
  statement: string;
  count_statement: string;
  values: Array<any>;
}

interface QueryEndpointPayload {
  /**
   * This is what we need to actually get a query back from the server.
   * 
   * TODO 2025-02-13: conn probably shouldn't be in here.
   */
  conn: duckdb.AsyncDuckDBConnection;
  tableName: string;
  filters: Array<Filter>;
  page: number;
  perPage: number
}

interface UnitializedTableState extends AlpineComponent<{}> {
  /**
   * Weird mirror to make the types play nice - lots of stuff is only defined after init() is called.
   */
  tableName: string | null;
  numRowsMatched: number | null;
  numRowsDisplayed: number;
  addedTables: Set<string>;
  showPreview: boolean;
  csvExportPageSize: number;
  exporting: boolean;
  gridApi: GridApi | null;
  db: duckdb.AsyncDuckDB | null;
  conn: duckdb.AsyncDuckDBConnection | null;
  exportCsv: () => void;
}

interface TableState extends AlpineComponent<{}> {
  /**
   * The strict version of the table state type, which has everything non-null.
   */
  tableName: string;
  numRowsMatched: number;
  numRowsDisplayed: number;
  addedTables: Set<string>;
  showPreview: boolean;
  csvExportPageSize: number;
  exporting: boolean;
  gridApi: GridApi;
  db: duckdb.AsyncDuckDB;
  conn: duckdb.AsyncDuckDBConnection;
  exportCsv: () => void;
}

const data: UnitializedTableState = {
  tableName: null,
  numRowsMatched: null,
  numRowsDisplayed: 0,
  addedTables: new Set(),
  showPreview: false,
  csvExportPageSize: 1_000_000,
  exporting: false,
  gridApi: null,
  db: null,
  conn: null,

  async init() {
    /**
     * Initialization function:
     *
     * - makes sure duckDB is alive
     * - makes an AG Grid
     * - attaches event handlers.
     */
    console.log("Initializing");

    this.db = await _initializeDuckDB();

    this.conn = await this.db.connect();
    await this.conn.query("SET default_collation='nocase';");

    const gridOptions: GridOptions = {
      onFilterChanged: async () => refreshTable(this as TableState)
    }
    const host = document.getElementById("data-table")!;
    this.gridApi = createGrid(host, gridOptions);
    this.$watch("tableName", () => refreshTable(this as TableState));
  },

  async exportCsv() {
    /**
     * Download data one giant page at a time, and then export to CSV.
     */
    const state = this as TableState;
    const { conn, tableName, gridApi, csvExportPageSize } = state;
    state.exporting = true;
    const numPages = Math.ceil(state.numRowsMatched / state.csvExportPageSize);

    for (let i = 1; i <= numPages; i++) {
      const filename = numPages === 1 ? tableName : `${tableName}_part${i}`;
      await exportPage(gridApi, filename, { conn, tableName, page: i, perPage: csvExportPageSize, filters: getFilters(gridApi) })
    }
    state.exporting = false;
  }
};

Alpine.data("tableState", () => data);
Alpine.start();

async function refreshTable(state: TableState) {
  /**
   * Re-query the data given the current table state.
   * 
   * TODO 2025-02-13 - since this mutates table state, maybe it should live in
   * the table state object too?
   * 
   * - check if the table has been registered - if not, register it.
   * - grab filters, table name, and get arrowData + a count back.
   * - turn arrowData into gridOptions.
   * - update the counters.
   * - throw the gridOptions at the gridApi.
   */
  const { tableName, conn, db, gridApi, addedTables } = state;

  if (tableName && !addedTables.has(tableName)) {
    await _addTableToDuckDB(db, tableName);
    addedTables.add(tableName);
  }
  const filters = getFilters(gridApi);
  const { arrowData, numRowsMatched } = await getAndCountData({ conn, tableName, filters, page: 1, perPage: 10_000 });
  const gridOptions = arrowTableToAgGridOptions(arrowData);
  gridApi.updateGridOptions(gridOptions);

  state.numRowsMatched = numRowsMatched;
  state.numRowsDisplayed = arrowData.numRows;
}

function getFilters(gridApi: GridApi): Array<Filter> {
  /**
   * Convert GridApi filter model to a list of Filters.
   * 
   * TODO 2025-02-13: if we start getting multiple filter conditions on each
   * column we will have to handle this differently - i.e. we'll have to retool
   * the Filter type altogether.
   */
  return Object.entries(gridApi.getFilterModel())
    .map(
      ([fieldName, { filterType, type, filter, filterTo }]) => (
        { fieldName, fieldType: filterType, operation: type, value: filter, valueTo: filterTo }
      )
    );
}

async function getAndCountData(params: QueryEndpointPayload) {
  /**
   * Get the data, and also count how many the full result would be.
   * 
   * - get the DuckDB query
   * - run the main query and the count query on DuckDB
   * - return both
   */
  const { conn, tableName, filters, page, perPage } = params;
  const { statement, count_statement: countStatement, values: filterVals } = await _getDuckDBQuery(
    { tableName, filters: filters, page, perPage }
  );
  const stmt = await conn.prepare(statement);
  const counter = await conn.prepare(countStatement);
  const [countResult, arrowData] = await Promise.all(
    [counter.query(...filterVals), stmt.query(...filterVals)]
  );
  const numRowsMatched = parseInt(countResult?.getChild("count_star()")?.get(0));

  return { arrowData, numRowsMatched }

}

async function getData(params: QueryEndpointPayload) {
  /**
   * Get the data, and also count how many the full result would be.
   * 
   * - get the DuckDB query
   * - run the main query on DuckDB
   */
  const { conn, tableName, filters, page, perPage } = params;
  const { statement, values: filterVals } = await _getDuckDBQuery(
    { tableName, filters: filters, page, perPage }
  );
  const stmt = await conn.prepare(statement);
  const arrowData = await stmt.query(...filterVals);
  return arrowData;
}


async function exportPage(gridApi: GridApi, filename: string, params: QueryEndpointPayload) {
  /**
   * Actually do the downloading/CSV export for a single page.
   *
   * - get data
   * - reshape it into CSV
   * - make a blob
   * - download it
   */
  const arrowTable = await getData(params);
  const { rowData } = arrowTableToAgGridOptions(arrowTable);

  const columns = gridApi.getColumns()?.map(col => col.colId) ?? [];
  const headers = columns.join(",");

  // get row values in the order of the columns passed in, then do one big string conversion using JSON.stringify.
  const rows = JSON.stringify(rowData!.map(row => columns.map(col => row[col])))
    .replace(/\],\[/g, '\n')
    .replace(/\[\[|\]\]/g, '');

  // make a binary file to download.
  const blob = new Blob([`${headers}\n${rows}`], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = `${filename}.csv`;
  link.click();
  URL.revokeObjectURL(url);
};

function arrowTableToAgGridOptions(table: arrow.Table): GridOptions {
  /**
   * Convert an Arrow table into something AG Grid can understand - a list of
   * records and a funny bespoke schema object (columnDefs).
   * 
   * We have to set some different options based on the type information in
   * Arrow - i.e. date formatting.
   *
   * TODO 2025-02-13: If we want to make a custom filter UI for specific types
   * (i.e. datetimes, categoricals) we'll need to set them in typeOpts.
   */
  const typeOpts = new Map([...TIMESTAMP_TYPE_IDS].map(tid => [tid, { valueFormatter: p => p.value?.toISOString() }]));
  const defaultOpts = { filter: true, filterParams: { maxNumConditions: 1, buttons: ["apply", "clear", "reset", "cancel"] } };

  const schema = table.schema;
  const columnDefs = schema.fields.map(
    f => ({
      ...defaultOpts,
      ...(typeOpts.get(f.type.typeId) ?? {}),
      field: f.name,
      headerName: f.name,
    })
  );
  const timestampColumns = schema.fields.filter(f => DATE_TS_TYPE_IDS.has(f.type.typeId)).map(f => f.name);
  const rowData = table.toArray().map(row => convertDatetimes(timestampColumns, row.toJSON()));
  return { columnDefs, rowData };
}

function convertDatetimes(timestampColumns: Array<string>, row: Object): Object {
  /**
   * Convert the integer timestamps that Arrow uses into JS Date objects.
   */
  timestampColumns.forEach(col => { row[col] = new Date(row[col]) });
  return row;
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


async function _getDuckDBQuery(
  { tableName, filters, page = 1, perPage = 10000 }
    : { tableName: string, filters: Array<Filter>, page?: number, perPage?: number }
): Promise<QuerySpec> {
  /**
   * Get DuckDB query from the backend, based on the filter rules & what table we're looking at.
   */
  const params = new URLSearchParams(
    {
      name: `${tableName}.parquet`,
      filters: JSON.stringify(filters),
      page: page.toString(),
      perPage: perPage.toString()
    }
  );
  const resp = await fetch("/api/duckdb?" + params);
  const query = await resp.json();
  console.log("QuerySpec:", query);
  return query
}