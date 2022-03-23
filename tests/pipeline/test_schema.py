import pytest
import toml

from shipping_emissions.pipeline.schema import XlsxColumn, XlsxSchema, load_xlsx_schema


@pytest.fixture
def xlsx_schema_definition():
    return {
        "first_row": 1,
        "columns": {
            "ship_name": {
                "index": 0,
                "original_name": "Ship Name",
                "type": "str",
            },
            "ship_length": {
                "index": 1,
                "original_name": "Ship Length (m)",
                "type": "int",
            },
        },
    }


@pytest.fixture
def xlsx_schema_file(tmp_path, xlsx_schema_definition):
    file_path = tmp_path / "schema.toml"
    with file_path.open("w") as o:
        toml.dump(xlsx_schema_definition, o)
    return file_path


@pytest.fixture
def xlsx_schema():
    return XlsxSchema(
        first_row=1,
        columns={
            XlsxColumn(name="ship_name", index=0, original_name="Ship Name", type="str"),
            XlsxColumn(name="ship_length", index=1, original_name="Ship Length (m)", type="int"),
        },
    )


def test_load_xlsx_schema(xlsx_schema_file, xlsx_schema):
    schema = load_xlsx_schema(xlsx_schema_file)
    assert dict(schema) == dict(xlsx_schema)  # can't compare models directly, not sure why
