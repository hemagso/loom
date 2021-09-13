from loom.tables import InputTable, DerivedTable
from loom.fields import RawField, DerivedField

t1 = InputTable("main", "table_1", "Table 1", "t1")
RawField(t1, "id", "Id")
RawField(t1, "value", "Value")
RawField(t1, "income", "Customer Income")

t3 = InputTable("main", "table_1", "Table 1", "t3")
RawField(t3, "id", "Id")
RawField(t3, "value", "Value")
RawField(t3, "income", "Customer Income")

t2 = DerivedTable("main", "table_2", "Table 2", "t2", [t1, t3])
DerivedField(t2, "id", "Id", "{t1.id}"),
DerivedField(t2, "abs_value", "Absolute Value", "abs({t1.id})")
DerivedField(t2, "div_income", "", "{t3.value}/{t1.income}")

print(t2.get_source("t1"))
print(t2.div_income.get_sources())
print(t2.get_select())