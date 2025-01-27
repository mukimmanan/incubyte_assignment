import pandas as pd
import psycopg2
import os
from sqlalchemy import create_engine


def raw_data_available(path):
    return os.listdir(path)


def read_data(exact_path):
    return pd.read_csv(exact_path, dtype=str, delimiter="|").drop(['Unnamed: 0'],axis=1)


def raw_data_dump(raw_data):
    raw_data.to_sql('patients_raw', engine, if_exists='replace', index=False)


def create_table_country_customer_mapping():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS patients_country_map (
            cust_ID BIGINT NOT NULL PRIMARY KEY,
            country char(5) NOT NULL
        )
    """)

    conn.commit()


def data_staging():
    cursor.execute(f"""
        DROP TABLE IF EXISTS patients_staging;
        CREATE TABLE IF NOT EXISTS patients_staging AS (
            SELECT * FROM (
                SELECT 
                    CAST("Customer_Id" AS BIGINT) cust_ID,
                    "Customer_Name" AS Name,
                    TO_DATE("Open_Date", 'YYYYMMDD') AS Open_date,
                    TO_DATE("Last_Consulted_Date", 'YYYYMMDD') AS Consult_dt,
                    "Vaccination_Id" AS vac_id,
                    "Dr_Name" as Dr_Name,
                    "State" as State,
                    "Country" AS country,
                    TO_DATE("DOB", 'DDMMYYYY') AS DOB,
                    "Is_Active" AS Active,
                    date_part('year', age(TO_DATE("DOB", 'DDMMYYYY'))) AS Age,
                    EXTRACT(DAY FROM (CURRENT_DATE::TIMESTAMP - TO_DATE("Last_Consulted_Date", 'YYYYMMDD'))) > 30 "days since last consulted > 30",
                    ROW_NUMBER() OVER (PARTITION BY "Customer_Id" ORDER BY "Last_Consulted_Date" DESC) RANK
                FROM patients_raw
                WHERE "Country" = '{country}'
            )
            WHERE RANK = 1
        );
    """)


def create_table_if_not_exists():
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS "patients_{country.lower()}" (
          cust_id BIGINT NOT NULL PRIMARY KEY,
          name varchar(255) NOT NULL,
          open_date date NOT NULL,
          consult_dt date NOT NULL,
          vac_id char(5) NOT NULL,
          dr_name varchar(45) DEFAULT NULL,
          state char(5) DEFAULT NULL,
          country char(5) NOT NULL,
          dob date DEFAULT NULL,
          active char(3) NOT NULL,
          age INT,
          "days since last consulted > 30" BOOLEAN DEFAULT FALSE
        )
        """
    )
    conn.commit()


def create_table_for_updated_country():
    cursor.execute("""
        DROP TABLE IF EXISTS records_to_remove;
        
        CREATE TABLE records_to_remove AS (
             select pcm.cust_id, pcm.country as prev_country, ps."Country" curr_country from patients_country_map pcm join patients_raw ps
             on pcm.cust_id = CAST(ps."Customer_Id" AS BIGINT)
             WHERE pcm.country <> ps."Country"
        )
    """)
    conn.commit()


def merge_records():
    cursor.execute(f"""
        MERGE INTO patients_{country} as target
        USING (SELECT * FROM patients_staging WHERE country = '{country}') updates
            ON target.cust_ID = updates.cust_id
        WHEN NOT MATCHED THEN
            INSERT (
                cust_id,
                name,
                open_date,
                consult_dt,
                vac_id,
                dr_name,
                state,
                country,
                dob,
                active,
                age,
                "days since last consulted > 30"
            )
            VALUES (
                updates.cust_id,
                updates.name,
                updates.open_date,
                updates.consult_dt,
                updates.vac_id,
                updates.dr_name,
                updates.state,
                updates.country,
                updates.dob,
                updates.active,
                updates.age,
                updates."days since last consulted > 30"
            )
        WHEN MATCHED AND updates.consult_dt > target.consult_dt THEN UPDATE SET 
            consult_dt = updates.consult_dt,
            open_date = updates.open_date,
            vac_id = updates.vac_id,
            dr_name = updates.dr_name,
            state = updates.state,
            active = updates.active,
            age = updates.age,
            "days since last consulted > 30" = updates."days since last consulted > 30";
    
        MERGE INTO patients_country_map as target
        USING patients_staging updates
            ON target.cust_ID = updates.cust_id
        WHEN 
            MATCHED AND target.country <> updates.Country THEN UPDATE SET
            Country = updates.country
        WHEN NOT MATCHED THEN
            INSERT (cust_ID, country)
            VALUES (updates.cust_id, updates.country);
    """)
    conn.commit()


def get_records_to_delete():
    return pd.read_sql("SELECT * FROM records_to_remove", engine)


def delete_records_from_table():
    cursor.execute(f"""
        DELETE FROM patients_{country}
        WHERE cust_ID IN (
            SELECT cust_id FROM records_to_remove
            WHERE prev_country = '{country.strip()}'
        )
    """)

    conn.commit()


if __name__ == "__main__":
    # Raw Folder
    raw_path = "./data/inc"

    # DB Creds
    DATABASE = "patients_db"
    USER = "DUMMY_USER"
    PASSWORD = "DUMMY_PWD"
    HOST = "localhost"
    PORT = 5432

    files = raw_data_available(raw_path)
    if len(files) == 0:
        print("No Raw Data Available")

    else:
        # Creating Postgres Engine
        engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')
        conn = psycopg2.connect(
            database=DATABASE,
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )

        cursor = conn.cursor()
        create_table_country_customer_mapping()


        for file in files:
            data = read_data(raw_path + f"/{file}")
            raw_data_dump(data)
            create_table_for_updated_country()

            countries = data["Country"].unique()
            for country in countries:
                create_table_if_not_exists()
                data_staging()
                merge_records()

            delete_records = get_records_to_delete()
            for country in delete_records["prev_country"].unique():
                delete_records_from_table()
