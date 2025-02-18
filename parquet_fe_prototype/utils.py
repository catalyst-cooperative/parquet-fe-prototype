"""Useful helper functions."""

import re

from frictionless import Package
from docutils.core import publish_parts


SPHINX_TAGS = re.compile(r":(?:ref|func|doc):`([^`]+)`")


def rst_to_html(rst: str) -> str:
    cleaned_rst = re.sub(SPHINX_TAGS, r"``\1``", rst)
    # this surrounds the HTML we want with a <main> and a <p> tag. strip those
    return publish_parts(cleaned_rst, writer_name="html5")["html_body"]


def clean_descriptions(datapackage: Package) -> Package:
    if datapackage.description:
        datapackage.description = rst_to_html(datapackage.description)
    for resource in datapackage.resources:
        resource.description = rst_to_html(resource.description)
        for field in resource.schema.fields:
            field.description = rst_to_html(field.description)
    return datapackage
