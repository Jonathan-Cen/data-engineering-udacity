# Udacity Data Engineering Nanodegree - Data Modelling with Apache Cassandra

### Student: Jonathan Cen

## Project Context

A startup called Sparkify wants to analyze the data they've been collecting on songs and user
activity on their new music streaming app. The analysis team is particularly interested in
understanding what songs users are listening to. Currently, there is no easy way to query the data
to generate the results, since the data reside in a directory of CSV files on user activity on the
app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song
play data to answer the questions, and wish to bring you on the project. Your role is to create a
database for this analysis. You'll be able to test your database by running queries given to you by
the analytics team from Sparkify to create the results.

## Project Overview

In this project, you'll apply what you've learned on data modeling with Apache Cassandra and
complete an ETL pipeline using Python. To complete the project, you will need to model your data by
creating tables in Apache Cassandra to run queries. You are provided with part of the ETL pipeline
that transfers data from a set of CSV files within a directory to create a streamlined CSV file to
model and insert data into Apache Cassandra tables.

We have provided you with a project template that takes care of all the imports and provides a
structure for ETL pipeline you'd need to process this data.

## ETL Process

1. Iterate through all csv files in the <code>event_data</code> directory and create a new csv file
   that contains all event data.

2. Connect to a local Apache Cassandra cluster and create a Keyspace

3. Create tables and insert records.

4. Verify records have been inserted into Apache Cassandra successfully.

5. Drop all tables.
