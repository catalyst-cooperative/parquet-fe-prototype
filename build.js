const esbuild = require("esbuild");
const fs = require("fs");
const path = require("path");

async function build() {
    await esbuild.build({
        entryPoints: ["src/index.ts"],
        plugins: [],
        outdir: "dist",
        format: "esm",
        bundle: true,
        target: "es2022",
        assetNames: "[name]",
    });

    fs.writeFileSync(
        path.join(__dirname, "dist/index.html"),
        fs.readFileSync(path.join(__dirname, "src/index.html")).toString()
    );
}

build();
