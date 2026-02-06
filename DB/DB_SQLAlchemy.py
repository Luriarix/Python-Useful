from Config.Config import *
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from urllib.parse import quote_plus


def get_engine() -> Engine | None:
    try:
        connection_string = (
            "mssql+pyodbc:///?odbc_connect="
            + quote_plus(
                f"DRIVER={readConfig('driver', section='Database', location=rf'DB\app_config.ini')};"
                f"SERVER={readConfig('server', section='Database', location=rf'DB\app_config.ini')};"
                # f"PORT={readConfig('port', section='Database', location=rf'DB\app_config.ini')}"
                f"DATABASE={readConfig('database', section='Database', location=rf'DB\app_config.ini')};"
                # f"UID={readConfig('user', section='Database', location=rf'DB\app_config.ini')};"
                # f"PWD={readConfig('password', section='Database', location=rf'DB\app_config.ini')}"
                f"Trusted_Connection=yes;"
            )
        )

        engine = create_engine(connection_string, fast_executemany=True)
        print("SQLAlchemy engine created successfully!")
        return engine

    except Exception as e:
        print(f"Error creating SQLAlchemy engine: {e}")
        return None


def execute_query(query: str, engine: Engine = get_engine()) -> pd.DataFrame | None:
    """
    Execute a SQL query and return results as a pandas DataFrame.
    """
    try:
        with engine.connect() as connection:
            if query.strip().lower().startswith("select"):
                df = pd.read_sql(text(query), connection)
                print("SELECT query executed successfully!")
                return df

            else:
                with connection.begin() as trans:
                    try:
                        connection.execute(text(query))
                        print(f"{query.split()[0].upper()} query executed successfully!")
                        return None
                    except Exception:
                        trans.rollback()
                        raise

    except Exception as e:
        print(f"Error executing query: {e}")
        return None


def select(columns='*', schema='dbo', table='test', where_clause=None, top_n: int = None):
    query = f"SELECT {'TOP ' + str(top_n) + ' ' if top_n is not None else ''}{columns} FROM {schema}.{table}"
    
    if where_clause:
        query += f" WHERE {where_clause}"

    try:
        print(f"Executing query: {query}")
        return execute_query(query)
    except Exception as e:
        print(f"Error in query execution: {e}")
        return None


def table_exists(table_name):
    query = f"""
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_NAME = '{table_name}'
    """
    result = execute_query(query)
    return result.iloc[0, 0] > 0 if result is not None else False


def create_table(table_name, columns):
    cols_def = ", ".join(f"[{col}] {dtype}" for col, dtype in columns.items())
    create_query = f"CREATE TABLE {table_name} ({cols_def});"
    print(f"Creating table {table_name} with columns {columns}")
    execute_query(create_query)


def pandas_dtype_to_sql(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BIT"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME"
    else:
        return "NVARCHAR(MAX)"


def insert(table, data: pd.DataFrame | dict, columns=None):
    engine = get_engine()
    if engine is None:
        print("Failed to get engine for insert.")
        return

    if isinstance(data, dict):
        data = pd.DataFrame([data])

    if not table_exists(table):
        cols_types = {col: pandas_dtype_to_sql(dtype) for col, dtype in data.dtypes.items()}
        create_table(table, cols_types)

    if columns is None:
        cols = data.columns.tolist()
    else:
        cols = columns

    sql = f"INSERT INTO {table} ({', '.join(f'[{col}]' for col in cols)}) VALUES ({', '.join(':' + col for col in cols)})"

    params = data[cols].to_dict(orient='records')

    try:
        with engine.begin() as connection:  # transaction begins
            connection.execute(text(sql), params)
        print(f"Inserted {len(data)} rows into {table} successfully in bulk.")
    except Exception as e:
        print(f"Bulk insert failed: {e}")
        print("Falling back to row-by-row insert...")

        with engine.begin() as connection:
            for idx, row in data.iterrows():
                try:
                    connection.execute(text(sql), {col: row[col] for col in cols})
                except Exception as row_e:
                    print(f"Error inserting row {idx}: {row_e}")
                    print(f"Value: {row}")
        print(f"Inserted {len(data)} rows into {table} with row-by-row fallback.")


def update(table, data: pd.DataFrame | dict, columns=None, where_clause=None):
    engine = get_engine()
    if engine is None:
        print("Failed to get engine for update.")
        return

    if isinstance(data, dict):
        data = pd.DataFrame([data])

    if columns is None:
        cols = data.columns.tolist()
    else:
        cols = columns

    set_clause = ", ".join(f"[{col}] = :{col}" for col in cols)
    sql = f"UPDATE {table} SET {set_clause}"
    if where_clause:
        sql += f" WHERE {where_clause}"

    params = data[cols].to_dict(orient='records')

    try:
        with engine.begin() as connection:
            connection.execute(text(sql), params)
        print(f"Updated {len(data)} rows in {table} successfully in bulk.")
    except Exception as e:
        print(f"Bulk update failed: {e}")
        print("Falling back to row-by-row update...")

        with engine.begin() as connection:
            for idx, row in data.iterrows():
                try:
                    connection.execute(text(sql), {col: row[col] for col in cols})
                except Exception as row_e:
                    print(f"Error updating row {idx}: {row_e}")
                    print(f"Value: {row}")
        print(f"Updated {len(data)} rows in {table} with row-by-row fallback.")


def delete(table, where_clause):
    sql = f"DELETE FROM {table} WHERE {where_clause}"
    execute_query(sql)


def drop_table(table):
    sql = f"DROP TABLE {table}"
    execute_query(sql)


def truncate_table(table):
    sql = f"TRUNCATE TABLE {table}"
    execute_query(sql)


if __name__ == "__main__":
    select()
