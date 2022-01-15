# Udacity Data Engineering Nanodegree - Capstone Project

### Student: Jonathan Cen

-   A research center decides to study what U.S. cities are among the most popular destinations for
    immigrants such that they can propose an infrastructure upgrade plan to the local government. An
    example of infrastructure upgrade could be constructing more large airports in states that are
    popular destinations of immigrants. As a data engineer at the research center, Jonathan is
    tasked to:

    -   build a <code>data pipeline</code> to ingest raw data
    -   construct both a Data Lake and a Data Warehouse to support analytical purposes for the
        research center's data science team.

-   The datasets available to the research center are:
    -   I94 Immigration Data - this data comes from the US National Tourism and Trade Office.
    -   U.S. City Demographic Data - this data comes from OpenDataSoft
    -   Airport Code Table, provides details such as types, location, state code of an airport. Only
        airports within the U.S. are of interest at this stage.

The project follows the follow steps:

-   Step 1: Scope the Project and Gather Data
-   Step 2: Explore and Assess the Data
-   Step 3: Define the Data Model
-   Step 4: Run ETL to Model the Data

### How to view?

Please clone the report and view the following two IPython notebooks:

-   Capstone Project - Step 1 & 2 - Data Lake.ipynb
-   Capstone Project - Step 3 & 4 - Cloud Data Warehouse.ipynb

### Project Structure

-   Raw Data:
    -   airport-codes_csv.csv
    -   us-cities-demographics.csv
    -   immigration_data_sample.csv
    -   data/
-   <code>Infra_as_code.py</code>: use infrastructure as code to provision an Amazon Redshift Data
    Warehouse programmatically.
-   <code>sql_queries.py</code>: sql queries for sql-to-sql ETL on data warehouse.
-   <code>valid_parameters.py</code>: lists of valid parameters to be used to clean the "I94
    Immigration Data" dataset.
-   <code>data_dictionaries/</code>: contains data dictionaries for all tables used in the data
    warehouse.
