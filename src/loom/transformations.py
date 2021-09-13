from __future__ import annotations
from inspect import cleandoc
from typing import Optional
from .fields import CURRENT_TABLE_CONTEXT, DerivedField, get_fields_from_expr


def from_single(expression: str) -> DerivedField:
    """
    This is a helper functions that makes it easier to create fields that are
    just simple selects or casts of a field. This function will create a derived
    field that inherits the previous field physical and logical name.

    The original field will me automatically detected from the expression used
    to create the new field.

    Args:
        expression (str): The expression that defines the field.

    Returns:
        DerivedField: The newly created field.
    """
    table = CURRENT_TABLE_CONTEXT[0]
    field_list = get_fields_from_expr(expression, table._sources)
    assert len(field_list) == 1
    field = field_list.pop()
    return DerivedField(
        field._pname,
        expression,
        lname=field._lname
    )


def book_feature(pname: str, flag_expr: str, time_expr: str, metric_expr: str,
                 aggregator: str, lname: Optional[str] = None) -> DerivedField:
    expression = cleandoc("""
        case
            when ({flag_expr}) and ({time_expr}) then {aggregator}({metric_expr})
            else null
        end
    """).format(flag_expr=flag_expr, time_expr=time_expr, metric_expr=metric_expr, aggregator=aggregator)
    return DerivedField(
        pname,
        expression,
        lname=lname
    )
