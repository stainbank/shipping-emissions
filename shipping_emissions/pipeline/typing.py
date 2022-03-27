from functools import partial
from typing import Any, Callable, Literal, Optional, overload

from shipping_emissions.pipeline.schema import XlsxColumn


def apply_type(value: Optional[str], column: XlsxColumn) -> Any:
    """Return `value`cast to the type defined by `column`, or None on failure if nullable."""
    # Simple initial implementation; really should look up a type caster configured by `column`
    cast = _get_type_caster(column)
    return cast(value)


def _get_type_caster(column: XlsxColumn) -> Callable[[Optional[str]], Any]:
    if cast := _TYPE_CASTERS.get(column.type):
        return partial(cast, nullable=column.nullable)
    else:
        return partial(_cast_column_type, column.type, nullable=column.nullable)


def _cast_column_type(column_type: str, value: Optional[str], nullable: bool) -> Any:
    type_: Callable = eval(column_type)
    try:
        return type_(value)
    except (TypeError, ValueError) as e:
        if nullable:
            return None
        else:
            raise e


@overload
def _cast_int(value: str, nullable: Literal[False]) -> int:
    ...


@overload
def _cast_int(value: str, nullable: Literal[True]) -> Optional[int]:
    ...


@overload
def _cast_int(value: None, nullable: Literal[True]) -> None:
    ...


def _cast_int(value: Optional[str], nullable: bool) -> Optional[int]:
    try:
        if value is None:
            raise TypeError
        return int(float(value))  # float to parse decimal, then trim it
    except (TypeError, ValueError) as e:
        if nullable:
            return None
        else:
            raise e


_TYPE_CASTERS: Callable[[Optional[str], bool], Any] = {
    "int": _cast_int,
}
