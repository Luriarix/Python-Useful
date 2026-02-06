from Config.Config import *
import pandas as pd
import pyodbc as db


def get_connection():
    conn_str = (
            f"DRIVER={readConfig('driver', section='Database', location=rf'DB\app_config.ini')};"
            f"SERVER={readConfig('server', section='Database', location=rf'DB\app_config.ini')};"
            # f"PORT={readConfig('port', section='Database', location=rf'DB\app_config.ini')}"
            f"DATABASE={readConfig('database', section='Database', location=rf'DB\app_config.ini')};"
            # f"UID={readConfig('user', section='Database', location=rf'DB\app_config.ini')};"
            # f"PWD={readConfig('password', section='Database', location=rf'DB\app_config.ini')}"
            f"Trusted_Connection=yes;"
        )
    return db.connect(conn_str)


def execute_query(query):
    try:
        with get_connection() as connection:
            print("Connection to database successful!")
            
            if query.strip().lower().startswith("select"):
                df = pd.read_sql_query(query, connection)
                print("SELECT query executed successfully!")
                return df
            
            else:
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    connection.commit()
                print(f"{query.split(' ')[0].upper()} query executed successfully!")
                return None

    except db.Error as e:
        print(f"Error executing query: {e}")
        return None


def select(columns='*', schema='dbo', table='test', where_clause=None, top_n: int = None):
    query = f"SELECT {'TOP ' + str(top_n) + ' ' if top_n is not None else ''}{columns} FROM {schema}.{table}"
    
    if where_clause:
        query += f" WHERE {where_clause}"

    try:
        print(f"Executing query: {query}")
        result = execute_query(query)
        return result
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
    """
    columns: dict of column_name: sql_data_type (e.g. {'id': 'INT', 'name': 'VARCHAR(255)'})
    """
    cols_def = ", ".join(f"[{col}] {dtype}" for col, dtype in columns.items())
    create_query = f"CREATE TABLE {table_name} ({cols_def});"
    print(f"Creating table {table_name} with columns {columns}")
    execute_query(create_query)


def pandas_dtype_to_sql(dtype):
    # Map pandas dtypes to SQL Server types, you can expand this as needed
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BIT"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME"
    else:
        return "NVARCHAR(MAX)"  # default to text


def insert(table, data: pd.DataFrame | dict, columns=None):
    
    if isinstance(data, dict):
        # Wrap single dict into DataFrame for consistent handling
        data = pd.DataFrame([data])

    if not table_exists(table):
        # Table doesn't exist, create it with columns from data
        cols_types = {col: pandas_dtype_to_sql(dtype) for col, dtype in data.dtypes.items()}
        create_table(table, cols_types)

    with get_connection() as connection:
        cursor = connection.cursor()

        if columns is None:
            cols = data.columns.tolist()
        else:
            cols = columns

        sql = f"INSERT INTO {table} ({', '.join(f'[{col}]' for col in cols)}) VALUES ({', '.join('?' for _ in cols)})"

        # Prepare parameter list
        params = data[cols].values.tolist()

        try:
            cursor.executemany(sql, params)
            connection.commit()
            print(f"Inserted {len(data)} rows into {table} successfully in bulk.")
        except Exception as e:
            print(f"Bulk insert failed: {e}")
            print("Falling back to row-by-row insert...")

            for idx, row in data.iterrows():
                try:
                    cursor.execute(sql, [row[col] for col in cols])
                except Exception as row_e:
                    print(f"Error inserting row {idx}: {row_e}")
                    print(f"Value: {row}")
            connection.commit()
            print(f"Inserted {len(data)} rows into {table} with row-by-row fallback.")


def update(table, data: pd.DataFrame | dict, columns=None, where_clause=None):
    if isinstance(data, dict):
        # Wrap single dict into DataFrame for consistent handling
        data = pd.DataFrame([data])

    with get_connection() as connection:
        cursor = connection.cursor()

        if columns is None:
            cols = data.columns.tolist()
        else:
            cols = columns

        set_clause = ", ".join(f"[{col}] = ?" for col in cols)
        sql = f"UPDATE {table} SET {set_clause}"
        if where_clause:
            sql += f" WHERE {where_clause}"

        params = data[cols].values.tolist()

        try:
            cursor.executemany(sql, params)
            connection.commit()
            print(f"Updated {len(data)} rows in {table} successfully in bulk.")
        except Exception as e:
            print(f"Bulk update failed: {e}")
            print("Falling back to row-by-row update...")

            for idx, row in data.iterrows():
                try:
                    cursor.execute(sql, [row[col] for col in cols])
                except Exception as row_e:
                    print(f"Error updating row {idx}: {row_e}")
                    print(f"Value: {row}")
            connection.commit()
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
