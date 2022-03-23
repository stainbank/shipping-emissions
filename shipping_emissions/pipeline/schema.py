import pathlib
import typing

import pydantic
import toml


class XlsxColumn(pydantic.BaseModel):
    index: int
    name: str
    original_name: typing.Optional[str] = None
    type: str

    class Config:
        frozen = True


class XlsxSchema(pydantic.BaseModel):
    first_row: int
    columns: set[XlsxColumn]


def load_xlsx_schema(schema_file: pathlib.Path) -> XlsxSchema:
    schema_def = toml.load(schema_file)
    column_defs = schema_def.pop("columns")
    columns = set(XlsxColumn(name=name, **column_def) for name, column_def in column_defs.items())
    return XlsxSchema(columns=columns, **schema_def)
