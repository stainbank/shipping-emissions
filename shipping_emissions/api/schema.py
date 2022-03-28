from typing import Any, Dict, Optional
import pyarrow as pa
import pyarrow.compute as pc
import strawberry

from shipping_emissions.db import db


LATEST_REPORTING_PERIOD = 2020
DATASET_NAME = "eu-mrv"


class DatabaseSession(strawberry.extensions.Extension):
    def on_request_start(self):
        self.execution_context.context["df"] = db.read(DATASET_NAME)


@strawberry.type
class Ship:
    data: strawberry.Private[Dict[str, Any]]

    @strawberry.field
    def imo_number(self) -> str:
        return self.data["imo_number"]

    @strawberry.field
    def reporting_period(self) -> int:
        return self.data["reporting_period"]

    @strawberry.field
    def name(self) -> str:
        return self.data["name"]


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
        return Ship(row)

    @strawberry.field
    def ships(
        info: strawberry.types.Info,
        imo_number: Optional[int] = None,
        reporting_period: Optional[int] = None,
        name: Optional[str] = None,
    ) -> list[Ship]:
        constraints = {}
        df = _filter_df(info.context["df"], **info.variable_values)
        # TODO: Implement pagination
        return [Ship(row) for row in df.to_pylist()]


def _filter_df(df: pa.Table, **constraints: Dict[str, Any]) -> pa.Table:
    for field, value in constraints.items():
        if value is not None:
            df = df.filter(pc.equal(df[field], value))
    return df


schema = strawberry.Schema(query=Query, extensions=[DatabaseSession])
