import typing
from shipping_emissions.pipeline.typing import apply_type

from shipping_emissions.pipeline.schema import XlsxSchema


def parse_xlsx(
    raw_data: typing.Iterable[typing.Iterable[typing.Optional[str]]], schema: XlsxSchema
) -> typing.Iterator[tuple[typing.Any, ...]]:
    for row in raw_data:
        # Parse the data
        columns = {column.index: column for column in schema.columns}
        yield tuple(apply_type(value, columns[i]) for i, value in enumerate(row))
