from collections.abc import Iterable
from typing import Any

import pandas as pd

from shipping_emissions.pipeline.schema import XlsxSchema

DATASETS = {}


class PandasDatabase:
    def __init__(self):
        self._tables: dict[str, pd.DataFrame] = {}

    def insert(self, rows: Iterable[tuple[Any, ...]], schema: XlsxSchema, dataset_name: str):
        """Insert `rows` with `schema` to specified DataFrame (creating if it doesn't exist)."""
        df = self._make_dataframe(rows, schema)
        dataset = self._tables.get(dataset_name)
        if dataset is None:
            dataset = df
        else:
            dataset = pd.concat([dataset, df], ignore_index=True)
        self._tables[dataset_name] = dataset

    def _make_dataframe(self, rows: Iterable[tuple[Any, ...]], schema: XlsxSchema) -> pd.DataFrame:
        """Create DataFrame from `rows` with `schema`."""
        return pd.DataFrame(
            list(rows),
            columns=[
                column.name for column in sorted(schema.columns, key=lambda column: column.index)
            ],
        )


db = PandasDatabase()
