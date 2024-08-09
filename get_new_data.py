from sqlalchemy import Engine
from sqlalchemy.sql import text
from datetime import datetime
import polars as pl

def get_new_data(engine: Engine, schema_name: str, table_name: str, last_modified_column: str, last_run_datetime: datetime) -> pl.DataFrame:
    """
    Fetches new or updated rows from the specified table based on the last modified datetime column.

    Parameters:
    engine (Engine): SQLAlchemy engine object connected to the source database.
    schema_name (str): The schema name where the table is located.
    table_name (str): The table name to fetch data from.
    last_modified_column (str): The column name that indicates the last modified datetime.
    last_run_datetime (datetime): The last time the data was fetched.

    Returns:
    pl.DataFrame: A Polars DataFrame containing the new or updated rows.
    """
    query: str = f"""
    SELECT * 
    FROM {schema_name}.{table_name}
    WHERE {last_modified_column} > :last_run_datetime
    """

    with engine.connect() as connection:
        result = connection.execute(text(query), {'last_run_datetime': last_run_datetime})
        rows = [dict(row) for row in result]
        new_data = pl.DataFrame(rows)
    
    return new_data
