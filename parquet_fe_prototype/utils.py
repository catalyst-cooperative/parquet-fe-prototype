"""Useful helper functions."""

import re

from frictionless import Package
from docutils.core import publish_parts


SPHINX_TAGS = re.compile(r":(?:ref|func|doc):`([^`]+)`")

EXTRA_HTML_TAGS = re.compile(r"</?(main|p)>")


def rst_to_html(rst: str) -> str:
    cleaned_rst = re.sub(SPHINX_TAGS, r"``\1``", rst)
    # this surrounds the HTML we want with a <main> and a <p> tag. strip those
    html_main = publish_parts(cleaned_rst, writer_name="html5")["html_body"]
    return re.sub(EXTRA_HTML_TAGS, "", html_main)


def clean_descriptions(datapackage: Package) -> Package:
    if datapackage.description:
        datapackage.description = rst_to_html(datapackage.description)
    for resource in datapackage.resources:
        resource.description = rst_to_html(resource.description)
        for field in resource.schema.fields:
            field.description = rst_to_html(field.description)
    return datapackage