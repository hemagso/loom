import sqlite3
import csv

def populate_db():
    con = sqlite3.connect("./sample_data/db.sqlite")
    cur = con.cursor()
    cur.execute("""
        create table raw_taxi_trips(
            id char(7),
            vendor_id char(1),
            pickup_datetime char(16),
            dropoff_datetime char(16),
            passengers char(1),
            pickup_longitude char(20),
            pickup_latitude char(20),
            dropoff_longitude char(20),
            dropoff_latitude char(20),
            store_and_fwd_flag char(1),
            trip_duration char(8)
        );
    """)
    
    with open("./sample_data/taxi-ny-data.csv", "r") as f:
        reader = csv.reader(f)
        next(reader)

        cur.executemany("""
            insert into raw_taxi_trips(
                id,
                vendor_id,
                pickup_datetime,
                dropoff_datetime,
                passengers,
                pickup_longitude,
                pickup_latitude,
                dropoff_longitude,
                dropoff_latitude,
                store_and_fwd_flag,
                trip_duration
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, reader)
    con.commit()
    con.close()


def main():
    populate_db()
    con = sqlite3.connect("./sample_data/db.sqlite")
    cur = con.cursor()
    cur.execute("select * from main.raw_taxi_trips limit 100")
    results = cur.fetchall()
    print(results)


if __name__ == "__main__":
    main()