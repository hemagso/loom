from loom.tables import DerivedTable, RawTable
from loom.fields import RawField, DerivedField
from loom.exceptions import DuplicatedFieldException, NoTableContextSetException 
import pytest

def test_create_table():
    table = RawTable("test_table_schema", "test_table_pname", lname="test_table_lname")
    assert table._schema == "test_table_schema"
    assert table._pname == "test_table_pname"
    assert table._lname == "test_table_lname"


def test_get_field_from_table():
    table = RawTable("test_schema", "test_pname", "test_lname")     
    with table:
        field = RawField("test_field_pname", "test_field_lname")
    assert table.test_field_pname is field

def test_create_derived_table():
    table_1 = RawTable("test_schema", "test_pname", "test_lname")     
    with table_1:
        RawField("test_field_pname", "test_field_lname")    
    table_2 = DerivedTable("test_schema", "test_pname", [table_1], "{test_pname}")

def test_create_derived_field():
    table_1 = RawTable("test_schema", "test_pname", "test_lname")     
    with table_1:
        field_1 = RawField("test_field_pname", "test_field_lname")    
    table_2 = DerivedTable("test_schema", "test_pname", [table_1], "{test_pname}")  
    with table_2:  
        field_2 = DerivedField("test_derived_pname", "{test_pname.test_field_pname}")
    assert table_2.test_derived_pname == field_2
    assert field_1 in field_2.get_sources()