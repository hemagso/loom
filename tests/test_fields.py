from loom.tables import DerivedTable, RawTable
from loom.fields import RawField, DerivedField
from loom.exceptions import DuplicatedFieldException, NoTableContextSetException 
import pytest


def test_create_field_nocontext():
    with pytest.raises(NoTableContextSetException):
        field = RawField("test_field_pname", lname="test_field_lname")


def test_create_field():
    table = RawTable("test_schema", "test_pname", "test_lname")
    with table:
        field = RawField("test_field_pname", "test_field_lname")

    assert field._table == table
    assert field._pname in table._fields
    assert table._fields[field._pname] == field


def test_add_multiple_fields():
    table = RawTable("test_schema", "test_pname", "test_lname")
    with table:
        field_1 = RawField("test_field1_pname", "test_field1_lname")
        field_2 = RawField("test_field2_pname", "test_field2_lname")

    assert "test_field1_pname" in table._fields
    assert "test_field2_pname" in table._fields
    assert table._fields[field_1._pname] == field_1
    assert table._fields[field_2._pname] == field_2


def test_add_duplicated_fields():
    table = RawTable("test_schema", "test_pname", "test_lname")     
    with pytest.raises(DuplicatedFieldException) as einfo:
        with table:
            RawField("test_field_pname", "test_field_lname")  
            field2 = RawField("test_field_pname", "test_field_lname")  
        assert einfo.value.table == table
        assert einfo.value.field == field2