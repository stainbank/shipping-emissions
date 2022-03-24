from collections.abc import Iterable, Iterator
from typing import Any, Optional

from shipping_emissions.pipeline.typing import apply_type
from shipping_emissions.pipeline.schema import XlsxSchema


def parse_xlsx(
    raw_data: Iterable[Iterable[Optional[str]]], schema: XlsxSchema
) -> Iterator[tuple[Any, ...]]:
    for row in raw_data:
        columns = {column.index: column for column in schema.columns}
        yield tuple(apply_type(value, columns[i]) for i, value in enumerate(row))
