# Project: Data Modeling with Postgres

## What is the purpose?

Sparkify is a startup that wants to analyze the data they've been gathering on songs and user activity on their recent music streaming app. The analytics team can now comprehend what songs users are listening using the python ETL script. It's a comfortable way to query their data, which resides in a directory of JSON logs on user actions on the app and a directory with JSON metadata on the songs in their app.

## What are the files in the repository?

**sql_queries.py**:
It contains all the SQL statements assigned to variables that allow the other python scripts (e.g., create_tables.py and etl.py) to drop and create tables, insert records, and perform queries.

**create_tables.py**:
Creates the database and connects it. Produces the database tables using sql_queries.py variable statements, can also drop the tables and create them again if they already exist (useful in case we need a new setup).

**etl.py**:
Performs the Extract, Transform and Load of the data.
EXTRACT data from its source (Data folder)
TRANSFORM data by deduplicating it, integrating it, and confirming quality.
LOAD data into the database.

## How to run the Python scripts
In this repository, you can encounter the following python scripts (create_tables.py, sql_queries.py, and etl.py) to test the ETL Sparkify uses to analyze the data.

First, we need to run create_tables.py it will create the necessary tables and database relationships to store the data.

Second and lastly, we need to run etl.py to get all the files stored in the folder data, extract the necessary information, transform the data and load it to the database.

## Database schema design

    // Fact Table
    Table songplays {
      songplay_id int [pk, NOT NULL]
      start_time timestamp
      user_id int
      level varchar
      song_id varchar
      artist_id varchar
      session_id int
      location varchar
      user_agent varchar
    }

    // Dimension Tables
    Table user {
      user_id int [pk, NOT NULL]
      first_name varchar
      last_name varchar
      gender varchar
      level varchar
    }


    Table song {
      song_id varchar [pk, NOT NULL]
      title varchar
      artist_id varchar
      year int
      duration float
    }

    Table artist {
      artist_id varchar [pk, NOT NULL]
      name varchar
      location varchar
      latitude float
      longitude float
    }

    Table time {
      start_time varchar
      hour int
      day int
      week int
      month int
      year int
      weekday int
    }
