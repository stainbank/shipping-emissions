from collections.abc import Iterator
from pathlib import Path
from typing import Optional

import openpyxl

from shipping_emissions.pipeline.schema import XlsxSchema


def read_xlsx(xlsx_file: Path, schema: XlsxSchema) -> Iterator[tuple[Optional[str], ...]]:
    """Yield rows in `xlsx_file` with non-null values coerced to str."""
    # Ideally would use read_only=True but it fails to load the entire workbook
    workbook = openpyxl.load_workbook(xlsx_file)
    try:
        for i, row in enumerate(workbook.active.iter_rows(values_only=True)):
            if i >= schema.first_row:
                yield tuple(str(val) if val is not None else val for val in row)
    finally:
        workbook.close()
