import pandas as pd
import sqlite3

def main():
    with sqlite3.connect("./sample_data/db.sqlite") as con:
        df = pd.read_sql("select * from main.book_taxi_trips limit 100", con)
        print(df)
        print(df.dtypes)

if __name__ == "__main__":
    main()