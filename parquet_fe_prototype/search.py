"""Interact with the document search."""

import re

from frictionless import Package, Resource

# TODO 2025-01-15: think about switching this over to py-tantivy since that's better maintained
from whoosh import index
from whoosh.analysis import RegexTokenizer, LowercaseFilter, StopFilter
from whoosh.fields import Schema, KEYWORD, TEXT, STORED
from whoosh.filedb.filestore import RamStorage
from whoosh.qparser import MultifieldParser
from whoosh.query import Or, Term


def initialize_index(datapackage: Package) -> index:
    """Index the resources from a datapackage for later searching.

    Search index is stored in memory since it's such a small dataset.
    """
    storage = RamStorage()

    analyzer = RegexTokenizer(r"[^\s_]+") | LowercaseFilter() | StopFilter()
    schema = Schema(
        name=TEXT(analyzer=analyzer),
        description=TEXT(analyzer=analyzer),
        columns=TEXT(analyzer=analyzer),
        tags=KEYWORD,
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
        tags = [resource.name.strip("_").split("_")[0]]
        if resource.name.startswith("_"):
            tags.append("preliminary")

        writer.add_document(
            name=resource.name,
            description=description,
            columns=columns,
            original_object=resource.to_dict(),
            tags=" ".join(tags),
        )

    writer.commit()

    return ix


def run_search(ix: index, raw_query: str) -> list[Resource]:
    """Actually run a user query.

    This doctors the raw query with some field boosts + tag boosts.
    """
    with ix.searcher() as searcher:
        parser = MultifieldParser(
            ["name", "description", "columns"],
            ix.schema,
            fieldboosts={"name": 2.0, "description": 1.0, "columns": 0.5},
        )
        query = parser.parse(raw_query)
        out_boost = Term("tag", "out", boost=2.0)
        preliminary_penalty = Term("tag", "preliminary", boost=2.0)
        results = searcher.search(Or([query, out_boost, preliminary_penalty]))
        return [hit["original_object"] for hit in results]
