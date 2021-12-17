import pandas as pd
import mysql.connector as msql
from mysql.connector import Error
import os

# Read CSVs
dir = os.getcwd()

asylumSeekersData = pd.read_csv(
    os.path.join(dir, "data/asylum_seekers.csv"), index_col=False, low_memory=False
)
asylumSeekersData.fillna(0, inplace=True)

asylumSeekersMonthly = pd.read_csv(
    os.path.join(dir, "data/asylum_seekers_monthly.csv"),
    index_col=False,
    low_memory=False,
)

asylumSeekersMonthly.fillna(0, inplace=True)

demographicsData = pd.read_csv(
    os.path.join(dir, "data/demographics.csv"), index_col=False, low_memory=False
)

demographicsData.fillna(0, inplace=True)

personsData = pd.read_csv(
    os.path.join(dir, "data/persons_of_concern.csv"),
    index_col=False,
    low_memory=False,
)

personsData.fillna(0, inplace=True)

resettlementData = pd.read_csv(
    os.path.join(dir, "data/resettlement.csv"), index_col=False, low_memory=False
)

resettlementData.fillna(0, inplace=True)

timeSeriesData = pd.read_csv(
    os.path.join(dir, "data/time_series.csv"), index_col=False, low_memory=False
)

timeSeriesData.fillna(0, inplace=True)

# specify database configurations
config = {
    "host": "localhost",
    "port": 3306,
    "user": "arudhatt",
    "password": "qaz",
    "database": "project_db",
}
db_user = config.get("user")
db_pwd = config.get("password")
db_host = config.get("host")
db_port = config.get("port")
db_name = config.get("database")

try:
    conn = msql.connect(host=db_host, database=db_name, user=db_user, password=db_pwd)
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

        # Table Asylum Seekers
        cursor.execute("DROP TABLE IF EXISTS asylum_seekers;")
        print("Creating table....")
        cursor.execute(
            """
           CREATE TABLE asylum_seekers (year int,
            country_of_asylum varchar(255),
            origin varchar(255),
            RSD_procedure_type varchar(255),
            total_pending int,
            UNHCR_assisted_start_year int,
            applied_during_year int,
            decisions_recognized int,
            decisions_other int,
            rejected int,
            otherwise_closed int,
            total_decisions int,
            total_pending_end_year int,
            UNHCR_assisted_end_year int)
           """
        )
        print("Asylum Seeker table is created....")
        for i, row in asylumSeekersData.iterrows():
            sql = "INSERT IGNORE INTO project_db.asylum_seekers VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            # print("Record inserted")
            # the connection is not autocommitted by default, so we must commit to save our changes
        conn.commit()

        # Table demographics
        cursor.execute("DROP TABLE IF EXISTS demographics;")
        print("Creating table....")
        cursor.execute(
            """
           CREATE TABLE demographics (year int,
            country_of_asylum varchar(255),
            location varchar(255),
            female_0_4 int,
            female_5_11 int,
            female_5_17 int,
            female_12_17 int,
            female_18_59 int,
            female_over_60 int,
            female_unknown float(12,5),
            female_total int,
            male_0_4 int,
            male_5_11 int,
            male_5_17 int,
            male_12_17 int,
            male_18_59 int,
            male_over_60 int,
            male_unknown float(12,5),
            male_total int)
           """
        )
        print("Demographics table is created....")
        for i, row in demographicsData.iterrows():
            sql = "INSERT IGNORE INTO project_db.demographics VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            # print("Record inserted")
            # the connection is not autocommitted by default, so we must commit to save our changes
        conn.commit()

        # Table Asylum Seekers Monthly
        cursor.execute("DROP TABLE IF EXISTS asylum_seekers_monthly;")
        print("Creating table....")
        cursor.execute(
            """
           CREATE TABLE asylum_seekers_monthly (
            country_of_asylum varchar(255),
            origin varchar(255),
            year int,
            month varchar(25),
            value int)
           """
        )
        print("Asylum Seekers Monthly table is created....")
        for i, row in asylumSeekersMonthly.iterrows():
            sql = "INSERT IGNORE INTO project_db.asylum_seekers_monthly VALUES (%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            # print("Record inserted")
            # the connection is not autocommitted by default, so we must commit to save our changes
        conn.commit()

        # Table persons_of_concern
        cursor.execute("DROP TABLE IF EXISTS persons_of_concern;")
        print("Creating table....")
        cursor.execute(
            """
           CREATE TABLE persons_of_concern (year int,
            country_of_asylum varchar(255),
            origin varchar(255),
            refugees int,
            asylum_seekers_pending int,
            refugees_returned int,
            internally_displaced_persons int,
            returned_idps int,
            stateless_persons int,
            others_of_concern int,
            total_population int)
           """
        )
        print("Persons of Concern table is created....")
        for i, row in personsData.iterrows():
            sql = "INSERT IGNORE INTO project_db.persons_of_concern VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            # print("Record inserted")
            # the connection is not autocommitted by default, so we must commit to save our changes
        conn.commit()

        # Table resettlement
        cursor.execute("DROP TABLE IF EXISTS resettlement;")
        print("Creating table....")
        cursor.execute(
            """
           CREATE TABLE resettlement (
            country_of_asylum varchar(255),
            origin varchar(255),
            year int,
            value int)
           """
        )
        print("Resettlement table is created....")
        for i, row in resettlementData.iterrows():
            sql = "INSERT IGNORE INTO project_db.resettlement VALUES (%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            # print("Record inserted")
            # the connection is not autocommitted by default, so we must commit to save our changes
        conn.commit()

        # Table time series
        cursor.execute("DROP TABLE IF EXISTS time_series;")
        print("Creating table....")

        cursor.execute(
            """
           CREATE TABLE time_series (year int,
            country_of_asylum varchar(255),
            origin varchar(255),
            population_type varchar(255),
            value int)
           """
        )
        print("Time Series table is created....")
        for i, row in timeSeriesData.iterrows():
            sql = "INSERT IGNORE INTO project_db.time_series VALUES (%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            # print("Record inserted")
            # the connection is not autocommitted by default, so we must commit to save our changes
        conn.commit()
except Error as e:
    print("Error while connecting to MySQL", e)
