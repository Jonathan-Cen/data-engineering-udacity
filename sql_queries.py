"""
sql_queries.py

Student: Jonathan Center
Description: this script contains necessary SQL statements for the Udacity Data Engineering Nanodegree Capstone Project
"""
import configparser

config = configparser.ConfigParser()
config.read('dwh-jc.cfg')

IAM_ROLE_ARN = config.get('IAM_ROLE', 'ARN')
AIRPORT_DATA = config.get('S3', 'AIRPORT_DATA')
IMMIGRATION_DATA = config.get('S3', 'IMMIGRATION_DATA')
DEMOGRAPHICS_DATA = config.get('S3', 'DEMOGRAPHICS_DATA')


# drop tables
airport_staging_drop = """ DROP TABLE IF EXISTS airport_staging """
immigration_staging_drop = """ DROP TABLE IF EXISTS immigration_staging  """
demographics_staging_drop = """ DROP TABLE IF EXISTS demographics_staging """
dim_airports_drop = """ DROP TABLE IF EXISTS dim_airports """
dim_demographics_drop = """ DROP TABLE IF EXISTS dim_demographics """
dim_travel_drop = """ DROP TABLE IF EXISTS dim_travel """
fact_immigration_records_drop = """ DROP TABLE IF EXISTS fact_immigration_records """

# create tables

airport_staging_create = """
    CREATE TABLE IF NOT EXISTS airport_staging
    (
        ident VARCHAR(15) NOT NULL PRIMARY KEY,
        type VARCHAR(20) NULL,
        name VARCHAR(100) NULL,
        elevation_ft NUMERIC NULL,
        continent VARCHAR(MAX) NULL,
        iso_country VARCHAR(10) NULL,
        municipality VARCHAR(MAX) NULL,
        gps_code VARCHAR(4) NULL,
        iata_code VARCHAR(3) NULL,
        local_code VARCHAR(8) NULL,
        state_code VARCHAR(2) NOT NULL,
        longitude NUMERIC NOT NULL,
        latitude NUMERIC NOT NULL
    )
"""

immigration_staging_create = """
    CREATE TABLE IF NOT EXISTS immigration_staging 
    (
        cicid INTEGER NOT NULL PRIMARY KEY,
        i94yr INTEGER NOT NULL,
        i94mon INTEGER NOT NULL,
        i94cit VARCHAR(MAX) NOT NULL,
        i94res VARCHAR(MAX) NOT NULL,
        i94port VARCHAR(MAX) NOT NULL,
        arrdate DATE NOT NULL,
        i94mode VARCHAR(10) NOT NULL,
        i94addr VARCHAR(2) NOT NULL,
        depdate DATE NOT NULL,
        i94bir INTEGER NOT NULL,
        i94visa INTEGER NOT NULL,
        matflag VARCHAR(1) NOT NULL,
        biryear INTEGER NOT NULL,
        gender VARCHAR(1) NOT NULL,
        airline VARCHAR(3) NOT NULL,
        fltno VARCHAR(10) NOT NULL,
        visatype VARCHAR(3) NOT NULL
    )
"""

demographics_staging_create = """
    CREATE TABLE IF NOT EXISTS demographics_staging
    (
        city VARCHAR(100) NOT NULL,
        state VARCHAR(20) NOT NULL,
        median_age NUMERIC NOT NULL,
        male_population INTEGER NOT NULL,
        female_population INTEGER NOT NULL,
        total_population INTEGER NOT NULL,
        num_of_veterans INTEGER NOT NULL,
        foreign_born INTEGER NOT NULL,
        average_household_size NUMERIC NOT NULL,
        state_code VARCHAR(2) NOT NULL,
        race VARCHAR(50) NOT NULL,
        count INTEGER NOT NULL
    )
"""

dim_airports_create = """
    CREATE TABLE IF NOT EXISTS dim_airports
    (
        airport_state_code VARCHAR(2) NOT NULL PRIMARY KEY,
        num_large_airports INTEGER NOT NULL
    )
"""

dim_demographics_create = """
    CREATE TABLE IF NOT EXISTS dim_demographics
    (
        state_code VARCHAR(2) NOT NULL PRIMARY KEY,
        male_population INTEGER NOT NULL,
        female_population INTEGER NOT NULL,
        total_population INTEGER NOT NULL,
        foreign_born INTEGER NOT NULL
    )
"""

dim_travel_create = """
    CREATE TABLE IF NOT EXISTS dim_travel
    (
        travel_date DATE NOT NULL PRIMARY KEY, 
        travel_year INTEGER NOT NULL, 
        travel_month INTEGER NOT NULL,
        travel_day INTEGER NOT NULL,
        travel_week INTEGER NOT NULL,
        travel_weekday INTEGER NOT NULL
    )
"""

fact_immigration_records_create = """
    CREATE TABLE IF NOT EXISTS fact_immigration_records
    (
        id INTEGER IDENTITY(0, 1) NOT NULL PRIMARY KEY,
        original_city VARCHAR(MAX) NOT NULL,
        port VARCHAR(MAX) NOT NULL,
        mode VARCHAR(10) NOT NULL,
        arrival_date DATE NOT NULL,
        departure_date DATE NOT NULL,
        gender VARCHAR(1) NOT NULL,
        state_code VARCHAR(2) NOT NULL,
        FOREIGN KEY (arrival_date) REFERENCES dim_travel(travel_date),
        FOREIGN KEY (departure_date) REFERENCES dim_travel(travel_date),
        FOREIGN KEY (state_code) REFERENCES dim_demographics(state_code),
        FOREIGN KEY (state_code) REFERENCES dim_airports(airport_state_code)
    )
"""


# staging tables copy

airport_staging_copy = ("""
    COPY airport_staging FROM '{}'
    credentials 'aws_iam_role={}'
    delimiter ','
    CSV
    ignoreheader 1
    maxerror as 250;
""").format(AIRPORT_DATA, IAM_ROLE_ARN)

immigration_staging_copy = ("""
    COPY immigration_staging FROM '{}'
    credentials 'aws_iam_role={}'
    delimiter ','
    CSV
    ignoreheader 1
    maxerror as 250;
""").format(IMMIGRATION_DATA, IAM_ROLE_ARN)

demographics_staging_copy = ("""
    COPY demographics_staging FROM '{}'
    credentials 'aws_iam_role={}'
    delimiter ','
    CSV
    ignoreheader 1
    maxerror as 250;
""").format(DEMOGRAPHICS_DATA, IAM_ROLE_ARN)

# insert queries

dim_airports_insert = """
    INSERT INTO dim_airports (airport_state_code, num_large_airports)
    SELECT 
        state_code, count(*) AS num_large_airports
    FROM 
        airport_staging
    WHERE
        type = 'large_airport'
    GROUP BY state_code
"""

dim_demographics_insert = """
    INSERT INTO dim_demographics (state_code, male_population, female_population, total_population, foreign_born)
    SELECT 
        state_code, MAX(male_population) as male_population, MAX(female_population) as female_population, MAX(total_population) as total_population, MAX(foreign_born) as foreign_born
    FROM 
        demographics_staging 
    GROUP BY 
        state_code 
    ORDER BY
        state_code
"""

dim_travel_insert_1 = """
    INSERT INTO dim_travel (travel_date, travel_year, travel_month, travel_day, travel_week, travel_weekday)
    SELECT DISTINCT
        arrdate AS travel_date,
        EXTRACT(year FROM arrdate) AS travel_year,
        EXTRACT(month FROM arrdate) as travel_month,
        EXTRACT(day FROM arrdate) AS day,
        EXTRACT(week FROM arrdate) AS travel_week,
        EXTRACT(DOW FROM arrdate) AS travel_weekday
    FROM 
        immigration_staging
    WHERE
        travel_date NOT IN (SELECT DISTINCT travel_date FROM dim_travel)
"""

dim_travel_insert_2 = """
    INSERT INTO dim_travel (travel_date, travel_year, travel_month, travel_day, travel_week, travel_weekday)
    SELECT DISTINCT
        depdate AS travel_date,
        EXTRACT(year FROM depdate) AS travel_year,
        EXTRACT(month FROM depdate) as travel_month,
        EXTRACT(day FROM depdate) AS day,
        EXTRACT(week FROM depdate) AS travel_week,
        EXTRACT(DOW FROM depdate) AS travel_weekday
    FROM 
        immigration_staging
    WHERE
        travel_date NOT IN (SELECT DISTINCT travel_date FROM dim_travel)
"""

fact_immigration_records_insert = """
    INSERT INTO fact_immigration_records (original_city, port, mode, arrival_date, departure_date, gender, state_code)
    SELECT
        i94cit AS original_city,
        i94port AS port,
        i94mode AS mode,
        arrdate AS arrival_date,
        depdate AS departure_date, 
        gender,
        i94addr AS state_code
    FROM 
        immigration_staging
"""

# Query lists
drop_table_queries = [airport_staging_drop, immigration_staging_drop, demographics_staging_drop, fact_immigration_records_drop, dim_airports_drop, dim_demographics_drop, dim_travel_drop]
create_table_queries = [airport_staging_create, immigration_staging_create, demographics_staging_create, dim_airports_create, dim_demographics_create, dim_travel_create, fact_immigration_records_create]
copy_table_queries = [airport_staging_copy, immigration_staging_copy, demographics_staging_copy]
insert_table_queries = [dim_airports_insert, dim_demographics_insert, dim_travel_insert_1, dim_travel_insert_2, fact_immigration_records_insert]
