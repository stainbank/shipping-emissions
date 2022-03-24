from collections.abc import Iterable
from pathlib import Path

from shipping_emissions.db import db
from shipping_emissions.pipeline.extract import read_xlsx
from shipping_emissions.pipeline.parse import parse_xlsx
from shipping_emissions.pipeline.schema import load_xlsx_schema


def run_pipeline(dataset_name: str, schema_file: Path, data_files: Iterable[Path]) -> None:
    """Read, parse and load dataset composed of `data_files` defined by `schema_file`."""
    schema = load_xlsx_schema(schema_file)
    for data_file in data_files:
        raw_data = read_xlsx(data_file, schema)
        parsed_data = parse_xlsx(raw_data, schema)
        db.append(parsed_data, schema, dataset_name)
