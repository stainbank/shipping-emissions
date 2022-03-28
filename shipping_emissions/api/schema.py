import dataclasses
from typing import Any, Dict, Mapping, Optional

import pyarrow as pa
import pyarrow.compute as pc
import strawberry

from shipping_emissions.db import db


LATEST_REPORTING_PERIOD = 2020
DATASET_NAME = "eu-mrv"
TABLE_TO_SHIP_FIELD_NAMES: Dict[str, str] = {
    "name": "name",  # temporary example
}


class DatabaseSession(strawberry.extensions.Extension):
    def on_request_start(self):
        self.execution_context.context["df"] = db.read(DATASET_NAME)


@strawberry.type
class Ship:
    @classmethod
    def from_data(cls, data: Mapping[str, Any]):
        fields = {}
        for field in dataclasses.fields(cls):
            data_field_name = TABLE_TO_SHIP_FIELD_NAMES.get(field.name, field.name)
            value = data[data_field_name]
            fields[field.name] = value
        return cls(**fields)

    imo_number: str
    reporting_period: int
    name: str


@strawberry.type
class Query:
    @strawberry.field
    def ship(
        info: strawberry.types.Info,
        imo_number: int,
        reporting_period: int = LATEST_REPORTING_PERIOD,
    ) -> Ship:
        df = info.context["df"]
        # TODO: Get constraints automatically from the context
        constraints = {"imo_number": imo_number, "reporting_period": reporting_period}
        df = _filter_df(info.context["df"], **constraints)
        (row,) = df.to_pylist()  # TODO: handle 0 or >1 results
        return Ship.from_data(row)

    @strawberry.field
    def ships(
        info: strawberry.types.Info,
        imo_number: Optional[int] = None,
        reporting_period: Optional[int] = None,
        name: Optional[str] = None,
    ) -> list[Ship]:
        df = _filter_df(info.context["df"], **info.variable_values)
        # TODO: Implement pagination
        return [Ship.from_data(row) for row in df.to_pylist()]


def _filter_df(df: pa.Table, **constraints: Dict[str, Any]) -> pa.Table:
    for field, value in constraints.items():
        if value is not None:
            df = df.filter(pc.equal(df[field], value))
    return df


schema = strawberry.Schema(query=Query, extensions=[DatabaseSession])
