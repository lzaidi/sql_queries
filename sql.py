import psycopg


def select_all(func):
    """
    Decorator that executes a SELECT query using the provided function and database connection parameters.

    Parameters:
    - func (function): Function representing a SELECT query.

    Returns:
    - function: Decorated function that executes the query and returns the result.
    """
    def execute(**kwargs):
        user = kwargs["user"]
        host = kwargs["host"]
        dbname = kwargs["dbname"]
        with psycopg.connect(f"user='{user}' host='{host}' dbname='{dbname}'") as conn:
            with conn.cursor() as curs:
                curs.execute(func(**kwargs))
                return curs.fetchall()
    return execute


def check_query_args(**kwargs):
    """
    Modifies the query based on optional arguments.

    Parameters:
    - kwargs (dict): Keyword arguments including 'query', 'explain', and 'n'.

    Returns:
    - str: Modified query.
    """
    query = kwargs['query']
    if 'explain' in kwargs and kwargs['explain'] is True:
        query = 'EXPLAIN ANALYZE VERBOSE ' + query
    if 'n' in kwargs:
        query = query + f" LIMIT {kwargs['n']}"
    return query


def commit(func):
    """
    Decorator that executes a query with commit operation using the provided function and database connection parameters.

    Parameters:
    - func (function): Function representing a query with commit operation.

    Returns:
    - function: Decorated function that executes the query and commits the changes.
    """
    def execute(**kwargs):
        user = kwargs["user"]
        host = kwargs["host"]
        dbname = kwargs["dbname"]
        isolation_level = kwargs["isolation_level"]
        with psycopg.connect(f"user='{user}' host='{host}' dbname='{dbname}'") as conn:
            conn.isolation_level = isolation_level
            with conn.cursor() as curs:
                curs.execute(func(**kwargs))
                return conn.commit()
    return execute


@commit
def create_view_incident_with_details(**kwargs):
    """
    Creates or replaces a view 'incident_with_details' in the database.

    Parameters:
    - kwargs (dict): Keyword arguments including database connection parameters.

    Returns:
    - str: Query to create the view.
    """
    query = '''
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
    return check_query_args(query=query, **kwargs)


@select_all
def daily_average_incident_increase(**kwargs):
    """
    Computes the daily average incident increase over a rolling seven-day period.

    Parameters:
    - kwargs (dict): Keyword arguments including database connection parameters.

    Returns:
    - str: Query to compute the daily average incident increase.
    """
    query = '''
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
    return check_query_args(query=query, **kwargs)


@select_all
def three_day_daily_report_type_ct(**kwargs):
    """
    Retrieves the report type counts for each day, including lag and lead counts.

    Parameters:
    - kwargs (dict): Keyword arguments including database connection parameters and 'year' and 'month'.

    Returns:
    - str: Query to retrieve report type counts.
    """
    year_month = f"{kwargs['year']}-{kwargs['month']:02d}"
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
                WHERE EXTRACT(YEAR from a.incident_datetime)={kwargs['year']}
                and EXTRACT(MONTH from a.incident_datetime) = {kwargs['month']}
                group by CAST(a.incident_datetime as date),
                b.report_type_description
                order by b.report_type_description,
                CAST(a.incident_datetime as date)) as sub
            '''
    return check_query_args(query=query, **kwargs)
