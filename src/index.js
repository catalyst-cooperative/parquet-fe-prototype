import perspective from "@finos/perspective";
import "@finos/perspective-viewer";
import "@finos/perspective-viewer-datagrid";
import "@finos/perspective-viewer-d3fc";

import "@finos/perspective-viewer/dist/css/pro-dark.css";
import "./index.css";

const viewer = document.createElement("perspective-viewer");
document.body.append(viewer);
const worker = await perspective.worker();
const table = worker.table({ x: [1, 2, 3, 4, 5] });

console.log(viewer);
viewer.load(table);
