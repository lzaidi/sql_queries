import psycopg


def select_all(func):
    def execute(**kargs):
        user = kargs["user"]
        host = kargs["host"]
        dbname = kargs["dbname"]
        with psycopg.connect(f"user='{user}' \
                            host='{host}' \
                            dbname='{dbname}'") as conn:
            with conn.cursor() as curs:
                curs.execute(func(**kargs))
                return curs.fetchall()
    return execute


def check_query_args(**kargs):
    query = kargs['query']
    if 'explain' in kargs and kargs['explain'] is True:
        query = 'EXPLAIN ANALYZE VERBOSE ' + query
    if 'n' in kargs:
        query = query + f" LIMIT {kargs['n']}"
    return query


def commit(func):
    """
    Q1. Complete the `commit()` decorator.
    This decorator should perform the following steps:
    a. Retrieve keyword arguments including
       `user`, `host`, `dbname`, and `isolation_level`.
    b. Create a connection using the `user`, `host`,
       and `dbname`, and set the isolation level.
    c. Execute a SQL query string returned from a function.
    d. Commit the changes.
    """

    def execute(**kargs):
        user = kargs["user"]
        host = kargs["host"]
        dbname = kargs["dbname"]
        isolation_level = kargs["isolation_level"]
        with psycopg.connect(f"user='{user}' \
                            host='{host}' \
                            dbname='{dbname}'") as conn:
            conn.isolation_level = isolation_level
            with conn.cursor() as curs:
                curs.execute(func(**kargs))
                return conn.commit()
    return execute


@commit
def create_view_incident_with_details(**kargs):
    """
    Q2. Create a view called incident_with_details, that includes id,
    incident_datetime, incident_code, incident_category,
    incident_subcategory, incident_description, longitude,
    latitude, report_datetime, report_type_code, report_type_description,
    supervisor_district, police_district and neighborhood
    for all the rows in incident table.
    """
    query = f'''
                CREATE OR REPLACE VIEW incident_with_details AS
                SELECT a.id, a.incident_datetime, a.incident_code,
                b.incident_category, b.incident_subcategory,
                b.incident_description,
                a.longitude, a.latitude, a.report_datetime,
                d.report_type_code, d.report_type_description,
                c.supervisor_district,
                c.police_district, c.neighborhood
                FROM incident a
                left join incident_type b
                on a.incident_code=b.incident_code
                left join location c
                on a.longitude = c.longitude and a.latitude=c.latitude
                left join report_type d
                on a.report_type_code=d.report_type_code
            '''
    return check_query_args(query=query, **kargs)


@select_all
def daily_average_incident_increase(**kargs):
    """
    Q3. Complete the daily_average_incident_increase() function.
    This function connects to a database using the parameters
    user, host, dbname, and n. It returns n records of date
    and average_incident_increase.
    The date represents the date of incident_datetime,
    and average_incident_increase indicates
    the difference between the average number of incidents
    in the previous 6 days and the current date, and
    the average number of incidents between the current date
    and the next 6 days.
    The value is rounded to 2 decimal points (as float) and
    the records are ordered by date.
    If the parameter n is not provided, the function returns all rows.
    """
    query = f'''
                SELECT
                date,
                (round(AVG(count) OVER(order by date ROWS
                BETWEEN 6 PRECEDING AND CURRENT ROW) -
                AVG(count) OVER(order by date ROWS BETWEEN CURRENT ROW
                and 6 following), 2)::FLOAT) as avg_incident_increase
                FROM (SELECT
                CAST(incident_datetime AS date) as date,
                COUNT(*)
                FROM incident
                Group by CAST(incident_datetime AS date)
                ORDER BY date) as counts
            '''
    return check_query_args(query=query, **kargs)


@select_all
def three_day_daily_report_type_ct(**kargs):
    """
    Q4. Complete the three_day_daily_report_type_ct() function.
    This function connects to a database using the parameters user,
    host, dbname, and n.
    It returns n records for all the incidents that occurred
    in the provided year and month.
    Each record includes the report_type_description,
    date, the number of incidents with the corresponding
    report_type_description one day before,
    the number of incidents with the corresponding
    report_type_description on the date,
    and the number of incidents with the corresponding
    report_type_description one day after.
    If the parameter n is not provided, the function returns all rows.
    """
    year_month = f"{kargs['year']}-{kargs['month']:02d}"
    query = f'''
                SELECT report_type_description, date,
                lag(count) OVER () as lag, count, lead(count) OVER ()
                FROM
                (SELECT b.report_type_description,
                CAST(a.incident_datetime as date) as date,
                count(report_type_description)
                FROM incident a
                left join report_type b
                on b.report_type_code = a.report_type_code
                WHERE EXTRACT(YEAR from a.incident_datetime)={kargs['year']}
                and EXTRACT(MONTH from a.incident_datetime) = {kargs['month']}
                group by CAST(a.incident_datetime as date),
                b.report_type_description
                order by b.report_type_description,
                CAST(a.incident_datetime as date)) as sub
            '''
    return check_query_args(query=query, **kargs)
