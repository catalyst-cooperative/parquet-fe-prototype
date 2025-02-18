from parquet_fe_prototype.utils import rst_to_html


def test_rst_to_html():
    really_bad_string = (
        ":ref:`reference` :doc:`document` :func:`function` normal text ``some code``"
    )
    expected_output = (
        "<main><p>"
        '<span class="docutils literal">reference</span>'
        " "
        '<span class="docutils literal">document</span>'
        " "
        '<span class="docutils literal">function</span>'
        " normal text "
        '<span class="docutils literal">some code</span>'
        "</p></main>"
    )
    assert rst_to_html(really_bad_string).replace("\n", "") == expected_output
