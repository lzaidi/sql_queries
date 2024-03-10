# SQL Query Script

## Overview

This Python script utilizes the psycopg library to execute SQL queries on a PostgreSQL database. It provides decorators for SELECT queries, modification of queries based on optional arguments, and executing queries with commit operations. The script includes example queries related to incident data, such as creating a view with details, computing daily average incident increase, and retrieving report type counts.

## Features

- **Decorator Functions:**
  - `select_all`: Decorator for executing SELECT queries and fetching results.
  - `commit`: Decorator for executing queries with commit operations.

- **Utility Function:**
  - `check_query_args`: Modifies the query based on optional arguments.

- **Example Queries:**
  - `create_view_incident_with_details`: Creates or replaces a view with detailed incident information.
  - `daily_average_incident_increase`: Computes the daily average incident increase over a rolling seven-day period.
  - `three_day_daily_report_type_ct`: Retrieves report type counts for each day, including lag and lead counts.

## Usage

1. **Install Dependencies:**
   ```bash
   pip install psycopg2
2. Run the script:
```
python sql_query_script.py
```
3. Connection parameters:
   - Modify the connection parameters (user, host, dbname, isolation_level) in the script according to your PostgreSQL database.

   
