from collections.abc import Iterable
from pathlib import Path
from typing import Any, Optional

import pyarrow as pa
import pyarrow.feather as feather

from shipping_emissions.pipeline.schema import XlsxSchema

DATASETS = {}


class FeatherDatabase:
    def __init__(self, data_dir):
        data_dir.mkdir(exist_ok=True)
        self._data_dir = data_dir

    def append(
        self, rows: Iterable[tuple[Any, ...]], schema: XlsxSchema, dataset_name: str
    ) -> None:
        """Append `rows` with `schema` to specified dataset, creating if it doesn't exist."""
        table = self._make_table(rows, schema)
        existing_table = self.read(dataset_name)
        if existing_table:
            table = pa.concat_tables([existing_table, table], promote=True)
        self._persist(table, dataset_name)

    def read(self, dataset_name: str) -> Optional[pa.Table]:
        """Return specified dataset, if it exists."""
        path = self._get_filepath(dataset_name)
        if path.exists():
            return feather.read_table(path)

    def _make_table(self, rows: Iterable[tuple[Any, ...]], schema: XlsxSchema) -> pa.Table:
        """Return table created from `rows` with `schema`."""
        names = [col.name for col in sorted(schema.columns, key=lambda col: col.index)]
        data = list(zip(*rows))  # rows -> columns
        return pa.table(data, names=names)  # this auto-types; really should pass a schema

    def _persist(self, dataset: pa.Table, dataset_name: str) -> None:
        """Write `dataset` to file named `dataset_name`."""
        path = self._get_filepath(dataset_name)
        feather.write_feather(dataset, path)

    def _get_filepath(self, dataset_name: str) -> Path:
        return self._data_dir / f"{dataset_name}.feather"


db = FeatherDatabase(Path("./data/db/"))
