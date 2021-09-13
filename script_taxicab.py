from loom.tables import DerivedTable, RawTable
from loom.fields import DerivedField, RawField, CURRENT_TABLE_CONTEXT
from loom.transformations import from_single
import sqlite3

def main():
    i_table = RawTable("main", "raw_taxi_trips", "Raw data for NY taxi trips", alias="a")
    with i_table:
        RawField("id", lname="unique identifier for each trip"),
        RawField("vendor_id", lname="code indicating the provider associated with the trip record"),
        RawField("pickup_datetime", lname="date and time when the meter was engaged"),
        RawField("dropoff_datetime", lname="date and time when the meter was disengaged"),
        RawField("passengers", lname="the number of passengers in the vehicle (driver entered value)"),
        RawField("pickup_longitude", lname="the longitude where the meter was engaged"),
        RawField("pickup_latitude", lname="the latitude where the meter was engaged"),
        RawField("dropoff_longitude", lname="the longitude where the meter was disengaged"),
        RawField("dropoff_latitude", lname="the latitude where the meter was disengaged"),
        RawField("store_and_fwd_flag", lname="This flag indicates whether the trip record was held in vehicle memory before sending to the vendor because the vehicle did not have a connection to the server - Y=store and forward; N=not a store and forward trip"),
        RawField("trip_duration", lname="duration of the trip in seconds")

    s_table = DerivedTable("main", "stage1_taxi_trips", "Data for NY taxi trips", "b", [i_table], "{a}")
    with s_table:
        from_single("{a.id}")    
        from_single("cast({a.vendor_id} as int)")
        from_single("{a.pickup_datetime}")
        from_single("{a.dropoff_datetime}"),
        from_single("cast({a.passengers} as int)"),
        from_single("cast({a.pickup_longitude} as real)"),
        from_single("cast({a.pickup_latitude} as real)"),
        from_single("cast({a.dropoff_longitude} as real)"),
        from_single("cast({a.dropoff_latitude} as real)"),
        from_single("cast({a.trip_duration} as int)")    
        DerivedField("delta_latitude", "abs({a.dropoff_latitude}-{a.pickup_latitude})", lname="Change in latitude")
        DerivedField("delta_longitude", "abs({a.dropoff_longitude}-{a.pickup_longitude})", lname="Change in longitude")

    b_table = DerivedTable("main", "book_taxi_trips", "Book for Taxi Trips", "c", [s_table], "{b}")
    with b_table:
        from_single("{b.vendor_id}")
        DerivedField("min_passengers", "min({b.passengers})", lname="Minimum of passengers")
        DerivedField("max_passengers", "max({b.passengers})", lname="Maximum of passengers")
    b_table.groupby([b_table.vendor_id])
    
    print(b_table.min_passengers.get_lineage())
    print(b_table.get_query())
    print(s_table.describe())
    with sqlite3.connect("./sample_data/db.sqlite") as con:
       pass
       cur = con.cursor()
       cur.execute("drop table book_taxi_trips;")
       cur.execute(b_table.get_query())
       con.commit()


if __name__ == "__main__":
    main()