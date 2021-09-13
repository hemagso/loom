from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .tables import BaseTable
    from .fields import BaseField


class DuplicatedFieldException(Exception):
    """
    An Exception that is raised when two fields with the same physical name are added
    to the same Table.

    Args:
        table (BaseTable): The table in which the error ocurred.
        field (BaseField): The field we tried to add to the table.
    """
    table: BaseTable
    field: BaseField

    def __init__(self, table: BaseTable, field: BaseField):
        self.table = table
        self.field = field
        message = f"Field {field._pname} already exists on table {table._pname}"
        super().__init__(message)


class UndefinedFieldException(Exception):
    """
    An Exception that is raised when we try to access a field that does not exist
    within a table.

    Args:
        table (BaseTable): The table in which we tried to acess the field
        field (str): The physical name we tried to access
    """
    table: BaseTable
    field: str

    def __init__(self, table: BaseTable, field: str):
        self.table = table
        self.field = field
        message = f"Field {field} was not defined in table {table._pname}"
        super().__init__(message)


class UnregisteredSourceException(Exception):
    table: BaseTable
    source: str

    def __init__(self, table: BaseTable, source: str):
        self.table = table
        self.source = source
        message = f"Source {source} was not registered with table {table}"
        super().__init__(message)


class NoTableContextSetException(Exception):
    def __init__(self, field: str):
        message = f"No table context set when creating field {field}"
        super().__init__(message)