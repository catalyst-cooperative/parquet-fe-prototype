import re

from frictionless import Package, Resource
from whoosh import index
from whoosh.fields import Schema, TEXT, STORED
from whoosh.filedb.filestore import RamStorage
from whoosh.qparser import MultifieldParser


def initialize_index(datapackage: Package) -> index:
    storage = RamStorage()

    schema = Schema(name=TEXT, description=TEXT, columns=TEXT, original_object=STORED)

    # Create in-memory index
    ix = storage.create_index(schema)
    writer = ix.writer()
    
    for resource in datapackage.resources:
        description = re.sub('<[^<]+?>', '', resource.description)
        columns = "".join((" ".join([field.name, field.description]) for field in resource.schema.fields))
        writer.add_document(name=resource.name, description=description, columns=columns, original_object=resource)

    writer.commit()
    return ix


def run_search(ix: index, raw_query: str) -> list[Resource]:
    with ix.searcher() as searcher:
        parser = MultifieldParser(["name", "content"], ix.schema)
        query = parser.parse(raw_query)
        results = searcher.search(query)
    return [r.original_object for r in results]

        
