import os
import pandas as pd
import mysql.connector

DB_CONFIG = {
    "host": "prevwage-db.cvcycq42gh41.us-east-2.rds.amazonaws.com",
    "port": 3306,
    "database": "prevwage_db",
    "user": "admin",
    "password": "EakJBpCrJLavky2",
}

def get_conn():
    return mysql.connector.connect(**DB_CONFIG)

def import_zips():
    df = pd.read_csv("zips.csv", dtype={"Zip": str})
    df.columns = [c.strip() for c in df.columns]

    df["Zip"] = df["Zip"].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(5)
    df["State"] = df["State"].astype(str).str.strip()
    df["County"] = df["County"].astype(str).str.strip()

    conn = get_conn()
    cur = conn.cursor()

    sql = """
        INSERT INTO zips (zip, state_name, county_name)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            state_name = VALUES(state_name),
            county_name = VALUES(county_name)
    """

    for _, row in df.iterrows():
        cur.execute(sql, (row["Zip"], row["State"], row["County"]))

    conn.commit()
    cur.close()
    conn.close()
    print("Zips imported")

def import_counties():
    df = pd.read_excel(
        "counties.xlsx",
        dtype={
            "State FIPS": str,
            "County FIPS": str,
            "FIPS": str,
        },
    )

    df.columns = [c.strip() for c in df.columns]
    df = df[df["County"].notna()].copy()

    df["State"] = df["State"].astype(str).str.strip()
    df["County"] = df["County"].astype(str).str.strip()
    df["Entity type"] = df["Entity type"].astype(str).str.strip()

    df["State FIPS"] = df["State FIPS"].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(2)
    df["County FIPS"] = df["County FIPS"].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(3)
    df["FIPS"] = df["FIPS"].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(5)

    conn = get_conn()
    cur = conn.cursor()

    sql = """
        INSERT INTO counties (
            state_name, county_name, entity_type, state_fips, county_fips, fips
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            state_name = VALUES(state_name),
            county_name = VALUES(county_name)
    """

    for _, row in df.iterrows():
        cur.execute(sql, (
            row["State"],
            row["County"],
            row["Entity type"],
            row["State FIPS"],
            row["County FIPS"],
            row["FIPS"],
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("Counties imported")

def import_wages():
    df = pd.read_csv("wages.csv")
    df.columns = [c.strip() for c in df.columns]

    df["fips"] = df["fips"].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(5)
    df["base_rate"] = df["base_rate"].astype(float)
    df["fringe_rate"] = df["fringe_rate"].astype(float)
    df["effective_date"] = pd.to_datetime(df["effective_date"]).dt.date

    conn = get_conn()
    cur = conn.cursor()

    sql = """
        INSERT INTO wages (
            fips, base_rate, fringe_rate, effective_date
        )
        VALUES (%s, %s, %s, %s)
    """

    for _, row in df.iterrows():
        cur.execute(sql, (
            row["fips"],
            row["base_rate"],
            row["fringe_rate"],
            row["effective_date"],
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("Wages imported")

if __name__ == "__main__":
    import_zips()
    import_counties()
    import_wages()
    print("All data imported!")