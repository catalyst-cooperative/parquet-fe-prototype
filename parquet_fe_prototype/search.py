import re

from frictionless import Package, Resource
from whoosh import index
from whoosh.analysis import RegexTokenizer, LowercaseFilter, StopFilter
from whoosh.fields import Schema, TEXT, STORED
from whoosh.filedb.filestore import RamStorage
from whoosh.qparser import MultifieldParser


def initialize_index(datapackage: Package) -> index:
    storage = RamStorage()

    analyzer = RegexTokenizer(r"[^\s_]+") | LowercaseFilter() | StopFilter()
    schema = Schema(
        name=TEXT(analyzer=analyzer),
        description=TEXT(analyzer=analyzer),
        columns=TEXT(analyzer=analyzer),
        original_object=STORED,
    )
    ix = storage.create_index(schema)
    writer = ix.writer()

    for resource in datapackage.resources:
        description = re.sub("<[^<]+?>", "", resource.description)
        columns = "".join(
            (
                " ".join([field.name, field.description])
                for field in resource.schema.fields
            )
        )
        writer.add_document(
            name=resource.name,
            description=description,
            columns=columns,
            original_object=resource.to_dict(),
        )

    writer.commit()

    return ix


def run_search(ix: index, raw_query: str) -> list[Resource]:
    with ix.searcher() as searcher:
        parser = MultifieldParser(
            ["name", "description", "columns"],
            ix.schema,
            fieldboosts={"name": 2.0, "description": 1.0, "columns": 0.5},
        )
        query = parser.parse(f"{raw_query} OR out")
        results = searcher.search(query)
        return [hit["original_object"] for hit in results]
