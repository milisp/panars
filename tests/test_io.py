import datetime as dt

import panars as pa

csv_path = "data.csv"


def test_dataframe():
    df = pa.DataFrame(
        {
            "name": ["Alice Archer", "Ben Brown", "Chloe Cooper", "Daniel Donovan"],
            "birthdate": [
                dt.date(1997, 1, 10),
                dt.date(1985, 2, 15),
                dt.date(1983, 3, 22),
                dt.date(1981, 4, 30),
            ],
            "weight": [57.9, 72.5, 53.6, 83.1],  # (kg)
            "height": [1.56, 1.77, 1.65, 1.75],  # (m)
        }
    )

    print(df)


def test_read_csv():
    pa.read_csv("data.csv")


def test_scan_csv():
    pa.scan_csv("data.csv")


def test_read_excel():
    df = pa.read_csv("data.csv")
    df.to_excel("/tmp/df.xlsx")
    pa.read_excel("/tmp/df.xlsx")


def test_read_parquet():
    df = pa.read_csv("data.csv")
    df.to_parquet("/tmp/df.parquet")
    pa.read_parquet("/tmp/df.parquet")
