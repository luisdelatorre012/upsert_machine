import get_new_data
from sqlalchemy import create_engine, Engine, text
from datetime import datetime
import polars as pl
from typing import Dict, List, Optional
import json

import upsert_data


def load_table_list_from_json(json_file: str) -> List[Dict[str, str]]:
    """
    Reads a list of table names with schemas and last modified columns from a JSON file.

    Parameters:
    json_file (str): Path to the JSON file.

    Returns:
    List[Dict[str, str]]: A list of dictionaries with 'schema', 'table', and 'last_modified_column' keys.
    """
    with open(json_file, "r") as file:
        config = json.load(file)
    return config.get("tables", [])


def get_last_run_datetime(
    target_engine: Engine, schema_name: str, table_name: str
) -> Optional[datetime]:
    """
    Retrieves the last run datetime from the target table in the target database.

    Parameters:
    target_engine (Engine): SQLAlchemy engine object connected to the target database.
    schema_name (str): The schema name of the target table.
    table_name (str): The name of the target table.

    Returns:
    Optional[datetime]: The last run datetime if found, otherwise None.
    """
    query = f"""
    SELECT MAX(last_run_datetime) as last_run_datetime
    FROM {schema_name}.{table_name}
    """

    with target_engine.connect() as connection:
        result = connection.execute(text(query)).fetchone()
        return result["last_run_datetime"] if result else None


def process_tables(
    source_engine: Engine, target_engine: Engine, json_file: str
) -> None:
    """
    Reads a list of table names with schemas and last modified columns from a JSON file,
    retrieves the last run datetime from the target database, fetches new data from the source database,
    and upserts the results into the target database.

    Parameters:
    source_engine (Engine): SQLAlchemy engine object connected to the source database.
    target_engine (Engine): SQLAlchemy engine object connected to the target database.
    json_file (str): Path to the JSON file.
    staging_table_name (str): The name of the staging table used for the upsert operation.
    key_columns (List[str]): A list of columns that make up the primary key or unique constraint.
    """

    tables = load_table_list_from_json(json_file)

    for table_info in tables:
        schema_name = table_info["schema"]
        table_name = table_info["name"]
        key_columns = table_info["key_columns"]
        last_modified_column = table_info["last_modified_column"]

        quoted_schema_name = f"[{schema_name}]"
        quoted_table_name = f"[{table_name}]"

        last_run_datetime = get_last_run_datetime(
            target_engine, quoted_schema_name, quoted_table_name
        )

        if last_run_datetime is None:
            last_run_datetime = datetime.min

        new_data = get_new_data(
            source_engine=source_engine,
            schema_name=quoted_schema_name,
            table_name=quoted_table_name,
            last_modified_column=last_modified_column,
            last_run_datetime=last_run_datetime,
        )

        upsert_result = upsert_data(
            engine=target_engine,
            new_data=new_data,
            schema_name=schema_name,
            table_name=table_name,
            key_columns=key_columns,
        )

        # TODO: you should probably use a logger to log the upsert result.


# Example usage
def main():
    # Example connections to SQL Server databases
    source_engine = create_engine(
        "mssql+pyodbc://user:password@source_server/source_database?driver=ODBC+Driver+17+for+SQL+Server"
    )
    target_engine = create_engine(
        "mssql+pyodbc://user:password@target_server/target_database?driver=ODBC+Driver+17+for+SQL+Server"
    )

    json_file = "tables_config.json"

    process_tables(source_engine, target_engine, json_file)


if __name__ == "__main__":
    main()
