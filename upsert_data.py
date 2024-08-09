from sqlalchemy import Engine, text
import polars as pl
from dataclasses import dataclass


@dataclass
class UpsertResult:
    rows_inserted: int
    rows_updated: int


def create_staging_table_if_not_exists(
    engine: Engine,
    target_schema: str,
    target_table: str,
    staging_schema: str,
    staging_table: str,
) -> None:
    """
    Checks if a staging table exists in the target database, and if it doesn't, creates the staging table
    by copying the schema from the target table.

    Parameters:
    engine (Engine): SQLAlchemy engine object connected to the target SQL Server database.
    target_schema (str): The schema name of the target table.
    target_table (str): The name of the target table.
    staging_schema (str): The schema name where the staging table should be created.
    staging_table (str): The name of the staging table to be created.

    Returns:
    None
    """

    check_query = f"""
    IF OBJECT_ID(N'{staging_schema}.{staging_table}', 'U') IS NULL
    BEGIN
        SELECT TOP 0 *
        INTO {staging_schema}.{staging_table}
        FROM {target_schema}.{target_table};
    END
    """

    with engine.connect() as connection:
        connection.execute(text(check_query))


def upsert_data(
    engine: Engine,
    dataframe: pl.DataFrame,
    schema_name: str,
    table_name: str,
    key_columns: list[str],
) -> UpsertResult:
    """
    Upserts data from a Polars DataFrame into a target table in a SQL Server database and returns the number of rows inserted and updated.

    This uses a staging table named named staging.schema_name_table_name.
    If this table doesn't exist, this function will create the table with the correct schema, but it will not include indexes or constraints.

    Parameters:
    engine (Engine): SQLAlchemy engine object connected to the target SQL Server database.
    dataframe (pl.DataFrame): The Polars DataFrame containing the data to be upserted.
    schema_name (str): The schema name where the target table is located.
    table_name (str): The name of the target table.

    Returns:
    UpsertResult: A dataclass containing the number of rows inserted and updated.
    """
    data = dataframe.to_dicts()

    columns = dataframe.columns

    column_placeholders = ", ".join(f":{col}" for col in columns)

    staging_table_full_name = f"[{schema_name}_{table_name}]"

    create_staging_table_if_not_exists(
        engine=engine,
        target_schema=schema_name,
        target_table=table_name,
        staging_schema="staging",
        staging_table=staging_table_full_name,
    )

    rows_inserted = 0
    rows_updated = 0

    with engine.connect() as connection:
        trans = connection.begin()
        try:
            truncate_query = f"TRUNCATE TABLE staging.{staging_table_full_name};"
            connection.execute(text(truncate_query))

            # Step 2: Bulk insert the data into the staging table
            insert_query = f"""
            INSERT INTO staging.{staging_table_full_name} ({', '.join(columns)})
            VALUES ({column_placeholders});
            """
            connection.execute(text(insert_query), data)

            merge_query = f"""
            DECLARE @inserted INT = 0;
            DECLARE @updated INT = 0;

            MERGE INTO {schema_name}.{table_name} AS target
            USING staging.{staging_table_full_name} AS source
            ON {' AND '.join([f'target.{col} = source.{col}' for col in key_columns])}
            WHEN MATCHED THEN
                UPDATE SET {', '.join([f'target.{col} = source.{col}' for col in columns if col not in key_columns])}
                WHEN MATCHED THEN
                    UPDATE SET @updated += 1
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ({', '.join(columns)}) VALUES ({', '.join(['source.' + col for col in columns])})
                OUTPUT $action INTO @inserted;

            SELECT @inserted AS RowsInserted, @updated AS RowsUpdated;
            """

            result = connection.execute(text(merge_query))
            for row in result:
                rows_inserted = row["RowsInserted"]
                rows_updated = row["RowsUpdated"]

            trans.commit()
        except Exception as e:
            trans.rollback()
            raise e

    return UpsertResult(rows_inserted=rows_inserted, rows_updated=rows_updated)
