from typing import Any, Dict
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
    imo_number: int
    reporting_period: int

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
        rows = df.filter(
            pc.and_(
                pc.equal(df["imo_number"], imo_number),
                pc.equal(df["reporting_period"], reporting_period),
            )
        )
        (row,) = rows.to_pylist()  # TODO: handle 0 or >1 results
        return Ship(row, imo_number, reporting_period)


schema = strawberry.Schema(query=Query, extensions=[DatabaseSession])
