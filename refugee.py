from IPython.display import display
import pandas as pd
import mysql.connector as msql
from mysql.connector import Error

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


def ref():

    try:
        conn = msql.connect(
            host=db_host, database=db_name, user=db_user, password=db_pwd
        )
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)

            print(
                """
            Choose one of the following queries:
            1 - Asylum Seekers Data
            2 - Asylum Seekers Monthly Data
            3 - Demographics Data
            4 - Persons of concern Data
            5 - Resettlement Data
            6 - Time Series Data
            """
            )
            ch = input()
            ch = ch.lstrip().rstrip()
            if ch == "1":
                sql1 = """SELECT * from asylum_seekers
                    limit 10 """
                cursor.execute(sql1)
                # Fetch all the records
                results = cursor.fetchall()
                # Returns a list of lists and then creates a pandas DataFrame
                from_db = []

                for result in results:
                    result = list(result)
                    from_db.append(result)

                columns = [
                    "year",
                    "country_of_asylum",
                    "origin",
                    "RSD_procedure_type",
                    "total_pending",
                    "UNHCR_assisted_start_year",
                    "applied_during_year",
                    "decisions_recognized",
                    "decisions_other",
                    "rejected",
                    "otherwise_closed",
                    "total_decisions",
                    "total_pending_end_year",
                    "UNHCR_assisted_end_year",
                ]
                df = pd.DataFrame(from_db, columns=columns)
                display(df)
            elif ch == "2":
                sql2 = """SELECT * from asylum_seekers_monthly
                    limit 10 """
                cursor.execute(sql2)
                # Fetch all the records
                results = cursor.fetchall()
                # Returns a list of lists and then creates a pandas DataFrame
                from_db = []

                for result in results:
                    result = list(result)
                    from_db.append(result)

                columns = ["country_of_asylum", "origin", "year", "month", "value"]
                df = pd.DataFrame(from_db, columns=columns)
                display(df)
            elif ch == "3":
                sql3 = """SELECT * from demographics
                    limit 10 """
                cursor.execute(sql3)
                # Fetch all the records
                results = cursor.fetchall()
                # Returns a list of lists and then creates a pandas DataFrame
                from_db = []

                for result in results:
                    result = list(result)
                    from_db.append(result)

                columns = [
                    "year",
                    "country_of_asylum",
                    "location",
                    "female_0_4",
                    "female_5_11",
                    "female_5_17",
                    "female_12_17",
                    "female_18_59",
                    "female_over_60",
                    "female_unknown",
                    "female_total",
                    "male_0_4",
                    "male_5_11",
                    "male_5_17",
                    "male_12_17",
                    "male_18_59",
                    "male_over_60",
                    "male_unknown",
                    "male_total",
                ]
                df = pd.DataFrame(from_db, columns=columns)
                display(df)
            elif ch == "4":
                sql4 = """SELECT * from persons_of_concern
                    limit 10 """
                cursor.execute(sql4)
                # Fetch all the records
                results = cursor.fetchall()
                # Returns a list of lists and then creates a pandas DataFrame
                from_db = []

                for result in results:
                    result = list(result)
                    from_db.append(result)

                columns = [
                    "year",
                    "country_of_asylum",
                    "origin",
                    "refugees",
                    "asylum_seekers_pending",
                    "refugees_returned",
                    "internally_displaced_persons",
                    "returned_idps",
                    "stateless_persons",
                    "others_of_concern",
                    "total_population",
                ]
                df = pd.DataFrame(from_db, columns=columns)
                display(df)
            elif ch == "5":
                sql5 = """SELECT * from resettlement
                    limit 10 """
                cursor.execute(sql5)
                # Fetch all the records
                results = cursor.fetchall()
                # Returns a list of lists and then creates a pandas DataFrame
                from_db = []

                for result in results:
                    result = list(result)
                    from_db.append(result)

                columns = ["country_of_asylum", "origin", "year", "value"]
                df = pd.DataFrame(from_db, columns=columns)
                display(df)
            elif ch == "6":
                sql6 = """SELECT * from time_series
                    limit 10 """
                cursor.execute(sql6)
                # Fetch all the records
                results = cursor.fetchall()
                # Returns a list of lists and then creates a pandas DataFrame
                from_db = []

                for result in results:
                    result = list(result)
                    from_db.append(result)

                columns = [
                    "year",
                    "country_of_asylum",
                    "origin",
                    "population_type",
                    "value",
                ]
                df = pd.DataFrame(from_db, columns=columns)
                display(df)
            else:
                print("Invalid Input")

    except Error as e:
        print("Error while connecting to MySQL", e)
