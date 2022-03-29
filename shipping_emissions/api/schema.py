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
    "additional_information": (
        "additional_information_to_facilitate_the_understanding_of_the_"
        "reported_average_operational_energy_efficiency_indicators"
    ),
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

    imo_number: Optional[str]
    reporting_period: Optional[int]
    ship_type: Optional[str]
    name: Optional[str]
    technical_efficiency: Optional[str]
    port_of_registry: Optional[str]
    home_port: Optional[str]
    ice_class: Optional[str]
    doc_issue_date: Optional[str]
    doc_expiry_date: Optional[str]
    verifier_number: Optional[str]
    verifier_name: Optional[str]
    verifier_nab: Optional[str]
    verifier_address: Optional[str]
    verifier_city: Optional[str]
    verifier_accreditation_number: Optional[str]
    verifier_country: Optional[str]
    monitoring_method_a: Optional[str]
    monitoring_method_b: Optional[str]
    monitoring_method_c: Optional[str]
    monitoring_method_d: Optional[str]
    total_fuel_consumption: Optional[float]
    fuel_consumptions_assigned_to_on_laden: Optional[float]
    total_co2_emissions: Optional[float]
    co2_emissions_from_all_voyages_between_ports_under_a_ms_jurisdiction: Optional[float]
    co2_emissions_from_all_voyages_which_departed_from_ports_under_a_ms_jurisdiction: Optional[
        float
    ]
    co2_emissions_from_all_voyages_to_ports_under_a_ms_jurisdiction: Optional[float]
    co2_emissions_which_occurred_within_ports_under_a_ms_jurisdiction_at_berth: Optional[float]
    co2_emissions_assigned_to_passenger_transport: Optional[float]
    co2_emissions_assigned_to_freight_transport: Optional[float]
    co2_emissions_assigned_to_on_laden: Optional[float]
    annual_total_time_spent_at_sea: Optional[float]
    annual_average_fuel_consumption_per_distance: Optional[str]
    annual_average_fuel_consumption_per_transport_work_mass: Optional[str]
    annual_average_fuel_consumption_per_transport_work_volume: Optional[str]
    annual_average_fuel_consumption_per_transport_work_dwt: Optional[str]
    annual_average_fuel_consumption_per_transport_work_pax: Optional[str]
    annual_average_fuel_consumption_per_transport_work_freight: Optional[str]
    annual_average_co2_emissions_per_distance: Optional[str]
    annual_average_co2_emissions_per_transport_work_mass: Optional[str]
    annual_average_co2_emissions_per_transport_work_volume: Optional[str]
    annual_average_co2_emissions_per_transport_work_dwt: Optional[str]
    annual_average_co2_emissions_per_transport_work_pax: Optional[str]
    annual_average_co2_emissions_per_transport_work_freight: Optional[str]
    through_ice: Optional[float]
    total_time_spent_at_sea: Optional[float]
    total_time_spent_at_sea_through_ice: Optional[float]
    fuel_consumption_per_distance_on_laden_voyages: Optional[str]
    fuel_consumption_per_transport_work_mass_on_laden_voyages: Optional[str]
    fuel_consumption_per_transport_work_volume_on_laden_voyages: Optional[str]
    fuel_consumption_per_transport_work_dwt_on_laden_voyages: Optional[str]
    fuel_consumption_per_transport_work_pax_on_laden_voyages: Optional[float]
    fuel_consumption_per_transport_work_freight_on_laden_voyages: Optional[float]
    co2_emissions_per_distance_on_laden_voyages: Optional[str]
    co2_emissions_per_transport_work_mass_on_laden_voyages: Optional[str]
    co2_emissions_per_transport_work_volume_on_laden_voyages: Optional[str]
    co2_emissions_per_transport_work_dwt_on_laden_voyages: Optional[str]
    co2_emissions_per_transport_work_pax_on_laden_voyages: Optional[float]
    co2_emissions_per_transport_work_freight_on_laden_voyages: Optional[float]
    additional_information: Optional[str]
    average_density_of_the_cargo_transported: Optional[float]


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
        constraints = {
            "imo_number": imo_number,
            "reporting_period": reporting_period,
            "name": name,
        }
        df = _filter_df(info.context["df"], **constraints)
        # TODO: Implement pagination
        return [Ship.from_data(row) for row in df.to_pylist()]


def _filter_df(df: pa.Table, **constraints: Dict[str, Any]) -> pa.Table:
    for field, value in constraints.items():
        if value is not None:
            df = df.filter(pc.equal(df[field], value))
    return df


schema = strawberry.Schema(query=Query, extensions=[DatabaseSession])
