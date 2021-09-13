from __future__ import annotations
from abc import ABC
from typing import Iterable, Dict, TYPE_CHECKING, Optional, Set
from inspect import cleandoc
from .exceptions import DuplicatedFieldException, UndefinedFieldException, UnregisteredSourceException
from .fields import CURRENT_TABLE_CONTEXT
if TYPE_CHECKING:
    from .fields import BaseField


class BaseTable(ABC):
    """
    This is an abstract class that defines common functionality for tables
    within Loom. This class provides some common functionality and must be
    inherited by other types of Tables.

    Args:
        schema (str): The name of the schema in which this table lives.
        pname (str): The physical name of the table (Actual name within the database)
        lname (str): The logical name of the table (Human readable name)
        alias (str, optional): An alias to be used when generating queries involving
            this table. If not specified the physical name is used.

    Tables on Loom use context managers to signal to fields which tables they
    are being created into.

    """
    _schema: str
    _pname: str
    _lname: str
    _alias: str
    _fields: Dict[str, BaseField]

    def __init__(self, schema: str, pname: str, lname: Optional[str] = None, alias: Optional[str] = None):
        self._schema = schema
        self._pname = pname
        self._lname = lname if lname else pname
        self._alias = alias if alias else pname
        self._fields = {}

    def __enter__(self):
        global CURRENT_TABLE_CONTEXT
        if len(CURRENT_TABLE_CONTEXT) > 0:
            raise RuntimeError()
        CURRENT_TABLE_CONTEXT.append(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        global CURRENT_TABLE_CONTEXT
        if self not in CURRENT_TABLE_CONTEXT:
            raise RuntimeError()
        CURRENT_TABLE_CONTEXT.remove(self)

    def add_field(self, field: BaseField):
        """
        Adds a field to the table

        Args:
            field (BaseField): Field to be added to the table

        TODO: Reduce coupling with BaseField
        """
        if field._pname in self._fields:
            raise DuplicatedFieldException(self, field)
        self._fields[field._pname] = field

    def describe(self, verbose: bool = False) -> str:
        fields = "\n    ".join(["- " + f.describe() for f in self._fields.values()])
        return cleandoc("""
            {lname}

            Schema: {schema}
            Physical Name: {pname}

            Fields:
                {fields}
        """).format(
            lname=self._lname,
            schema=self._schema,
            pname=self._pname,
            fields=fields
        )

    def __getitem__(self, key: str) -> BaseField:
        if key not in self._fields:
            raise UndefinedFieldException(self, key)
        return self._fields[key]

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        return f"{self._schema}.{self._pname} as {self._alias}"

    def __getattr__(self, key: str):
        return self.__getitem__(key)


class RawTable(BaseTable):
    pass


class DerivedTable(BaseTable):
    """
    This class represents Tables that are created from SQL queries from other tables.

    Args:
        schema (str): The name of the schema in which this table lives.
        pname (str): The physical name of the table (Actual name within the database)
        lname (str): The logical name of the table (Human readable name)
        alias (str, optional): An alias to be used when generating queries involving
            this table. If not specified the physical name is used.
    """
    _sources: Dict[str, BaseTable]
    _source_expression: str
    _groupby: Set[BaseField]

    def __init__(self, schema: str, pname: str, sources: Iterable[BaseTable], from_expression: str, 
                 lname: Optional[str] = None, alias: Optional[str] = None):
        super().__init__(schema, pname, lname=lname, alias=alias)
        self._groupby = set()
        self._sources = self._process_sources(sources)
        self._from_expression = from_expression

    @staticmethod
    def _process_sources(sources: Iterable[BaseTable]) -> Dict[str, BaseTable]:
        """
        This method process the iterable of BaseTables received by the constructor,
        formatting it as a "alias" to "table" mapping for easier acess.

        Args:
            sources (Iterable[BaseTable]): Iterable with all tables that are 
                used as sources for another table.

        Returns:
            Dict[str, BaseTable]: Dictionary mapping a table "alias" to the
                actual table object.
        """
        return {t._alias: t for t in sources}

    def groupby(self, fields: Iterable[BaseField]) -> None:
        """
        Sets the fields that are used as keys for aggregations for this table
        creation.

        Args:
            fields (Iterable[BaseField]): Iterable containing the fields that
                will be used for the group by.
        """
        self._groupby = set(fields)

    def get_source(self, key: str) -> BaseTable:
        if key not in self._sources:
            raise UnregisteredSourceException(self, key)
        return self._sources[key]

    def get_select(self, indent: int = 0) -> str:
        """
        Generates the select SQL expression for this table based on the fields added to it

        Args:
            indent (int, optional): Indent level for the statements, in whitespaces (Default: 0)
        """
        statement = ",\n".join([f.get_expression() for f in self._fields.values()])
        return statement.replace("\n", "\n"+indent*" ")

    def get_from(self) -> str:
        """
        Generates the from SQL expression for this table based on the source
        tables.

        Returns:
            str: String containing the SQL from expression.
        """
        return self._from_expression.format(**self._sources)

    def get_groupby(self) -> str:
        """
        Generates the group by SQL expression for this table based on the fields
        listed as group by keys.

        Returns:
            str: String containing the SQL group by expression.
        """
        if len(self._groupby) == 0:
            return ""
        statement = ",\n".join([f._pname for f in self._groupby])
        return f"group by \n {statement}"

    def get_query(self) -> str:
        """
        Generates the query that create the table.

        Returns:
            str: String containing the query.
        """
        return cleandoc("""
            create table {new_table} as
            select
                {select_expr}
            from
                {from_expr}
            {where_expr}
            {groupby_expr};
        """).format(
            new_table=self._pname,
            select_expr=self.get_select(indent=4),
            from_expr=self.get_from(),
            where_expr="",
            groupby_expr=self.get_groupby()
        )
