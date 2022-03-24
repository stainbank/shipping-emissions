from typing import Any, Optional

from shipping_emissions.pipeline.schema import XlsxColumn


def apply_type(value: Optional[str], column: XlsxColumn) -> Any:
    """Return `value`cast to the type defined by `column`, or None if that fails."""
    # Simple initial implementation; really should look up a type caster configured by `column`
    type_ = eval(column.type)
    try:
        return type_(value)
    except (TypeError, ValueError) as e:
        if column.nullable:  # at the moment everything is
            # Really this should be handled by type caster to allow specific null values
            return None
        else:
            raise e
