"Take the PUDL metadata and turn it into a frictionless datapackage."

from frictionless import Package, Field, Resource, Schema


# TODO 2024-12-17: we *could* make the yaml more type constrained. But we
# *could* also just write out a datapackage.json from PUDL in the future. in
# which case we can just get rid of all this custom logic.
def metadata_to_datapackage(yaml_dict: dict[str, any]) -> Package:
    pudl_dict = yaml_dict["databases"]["pudl"]
    resources = [
        table_to_resource(name, table) for name, table in pudl_dict["tables"].items()
    ]
    return Package(resources=resources)


def table_to_resource(name, table) -> Resource:
    return Resource(
        name=name,
        description=table["description_html"],
        schema=Schema(
            fields=[
                Field(name=colname, description=desc)
                for colname, desc in table["columns"].items()
            ]
        ),
    )
