from __future__ import annotations
from typing import List, Mapping, TYPE_CHECKING, Set, Optional
from abc import ABC, abstractmethod
from string import Formatter
from .exceptions import NoTableContextSetException
if TYPE_CHECKING:
    from .tables import BaseTable

# TODO: Create a fully fledged context manager for handling field creation
CURRENT_TABLE_CONTEXT: List[BaseTable] = []


class BaseField(ABC):
    """
    This class is an abstract class that represents a Field in a table.

    Args:
        pname (str): A field physical name. This is the actual name that the
            field has within the database.
        lname (str, optional): A field logical name. Logical names are human
            readable names for a field. This is optional, and if not used the
            physical name will be also used as the field logical name.

    Attributes:
        _pname (str): A field physical name. This is the actual name that the
            field has within the database.
        _lname (str): A field logical name. Logical names are human
            readable names for a field. This is optional, and if not used the
            physical name will be also used as the field logical name.
        _table (BaseTable): The table that contains the field.

    Not that the field constructor does not ask for the table name. This happens
    because a field needs always to be created within a context manager, as seen
    in the example below:

        table = RawTable('test_schema', 'test_table')
        with table:
            RawField('test_field_1')
            RawField('test_field_2')

    """

    _pname: str
    _lname: str
    _table: BaseTable

    def __init__(self, pname: str, lname: Optional[str] = None):
        self._pname = pname
        self._lname = lname if lname else pname
        self._table = self.set_table()

    def set_table(self) -> BaseTable:
        """
        This method set the parent table of the field according to the current
        selected table in the current table context.

        Returns:
            BaseTable: The table that was retrieved from the table context.
        """
        if len(CURRENT_TABLE_CONTEXT) == 0:
            raise NoTableContextSetException(self._pname)
        table = CURRENT_TABLE_CONTEXT[0]
        table.add_field(self)
        return table

    @abstractmethod
    def get_expression(self) -> str:
        """
        Returns the expression used to create the field

        Returns:
            str: A string containing a SQL statement that creates the field.
        """

    @abstractmethod
    def get_sources(self) -> Set[BaseField]:
        """
        Returns a set of fields that are used to create this field. This method
        goes back only a single step in the pipeline. Use get_lineage to go
        back to Input table.

        Returns:
            Set[BaseField]: A set containing the fields
        """

    @abstractmethod
    def get_lineage(self) -> Set[BaseField]:
        """
        Returns a set of fields that are used to create this field, going all
        the way back to source tables.

        Returns:
            Set[BaseField]: A set containing the fields
        """

    def describe(self) -> str:
        return f"{self._pname}: {self._lname}"

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        if self._table:
            return f"{self._table._alias}.{self._pname}"
        else:
            return f"{self._pname}"


class RawField(BaseField):
    """
    This class represents a table that, for the purposes of the pipeline
    being built, is already materialized in the database. This might be a
    file that was loaded from outside the database or a table that was
    created by another pipeline. A RawField does not store information
    about how it was created, and considers it to be its on source.

    Args:
        See documentation for BaseField.

    Attributes:
        See documentation for BaseField.
    """
    def get_sources(self) -> Set[BaseField]:
        return set([self])

    def get_lineage(self) -> Set[BaseField]:
        return set([self])

    def get_expression(self) -> str:
        return self._pname


class DerivedField(BaseField):
    """
    This class represents a Field that is created as a result of a SQL or SQL-
    like (HQL, Spark SQL) statement.

    Args:
        expression (str): String with the expression used to create the field.
        See also documentation for BaseField for other args.

    Attributes:
        _expression (str): Expression used to create the field.
        _sources (Set[BaseField]): Fields that are used to calculate this field.

    When writing expressions one should use string formatter fields to refer to
    other fields. For example, imagine you want to take the absolute value from
    another field "income" that comes from a table with alias "t1". The 
    following string represents a valid expression to do that:

        "abs({t1.income})"        

    """
    _expression: str
    _sources: Set[BaseField]

    def __init__(self, pname: str, expression: str, lname: Optional[str] = None):
        super().__init__(pname, lname=lname)
        self._expression = expression
        self._sources = get_fields_from_expr(expression, self._table._sources)

    def get_expression(self) -> str:
        """
        This method returns a SQL expression that creates the field, replacing
        placeholders with the actual physical names of the fields used in the
        expression.

        Returns:
            str: A SQL expression to create the field.
        """
        expression = self._expression.format(**self._table._sources)
        return f"{expression} as {self._pname}"

    def get_sources(self) -> Set[BaseField]:
        """
        Returns a set of fields that are used to create this field. This method
        goes back only a single step in the pipeline. Use get_lineage to go
        back to Input table.

        Returns:
            Set[BaseField]: A set containing the fields
        """
        return self._sources

    def get_lineage(self) -> Set[BaseField]:
        """
        Returns a set of fields that are used to create this field, going all
        the way back to source tables.

        Returns:
            Set[BaseField]: A set containing the fields
        """        
        lineage: Set[BaseField] = set()
        for f in self._sources:
            lineage = lineage | f.get_lineage()
        return lineage


def get_fields_from_expr(expression: str, context: Mapping[str, BaseTable]) -> Set[BaseField]:
    """
    This function returns a set with all the fields used in an expression. The
    expression string is parsed in search of identifiers, and the actual objects
    are looked on a provided mapping context.

    Args:
        expression (str): The expression being parsed.
        context (Mapping[str, BaseTable]): A mapping listing all tables that can
            provide fields used in the expression. This is usually set as the
            tables registered as the source for another table.
    """
    f = Formatter()
    field_list = set()
    for _, field_name, _, _ in f.parse(expression):
        if field_name:
            field, _ = f.get_field(field_name, [], context)
            field_list.add(field)
    return field_list
