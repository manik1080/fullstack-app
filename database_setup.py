import os
import pandas as pd
import mysql.connector
from tqdm import tqdm
from config import Passwords

def load_csv_folder_to_mysql(
    directory: str,
    host: str,
    user: str,
    password: str,
    database: str
):
    # map pandas dtypes to MySQL column types
    sql_type_map = {
        'object':          'TEXT',
        'int64':           'BIGINT',
        'float64':         'DOUBLE',
        'bool':            'BOOLEAN',
        'datetime64[ns]':  'DATETIME'
    }

    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    cursor = conn.cursor()

    for fname in os.listdir(directory):
        if not fname.endswith('.csv'):
            continue

        table = os.path.splitext(fname)[0]
        df = pd.read_csv(os.path.join(directory, fname))

        # --- convert all NaN/NaT to None so they become SQL NULLs ---
        df = df.where(pd.notnull(df), None)

        # --- 1) CREATE TABLE IF NOT EXISTS ---
        cols_defs = []
        for i, (col, dtype) in enumerate(df.dtypes.items()):
            if (i == 0 and (col == 'id' or (table == 'orders' and col == 'order_id'))):
                # Set as primary key auto_increment
                sql_type = 'BIGINT PRIMARY KEY AUTO_INCREMENT'
            else:
                sql_type = sql_type_map.get(str(dtype), 'TEXT')
            cols_defs.append(f"`{col}` {sql_type}")
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS `{table}` (
              {', '.join(cols_defs)}
            ) ENGINE=InnoDB;
        """
        cursor.execute(create_sql)

        # --- 2) Build a fixed placeholders string ---
        col_names = ", ".join(f"`{c}`" for c in df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_sql = f"INSERT INTO `{table}` ({col_names}) VALUES ({placeholders})"

        # --- 3) Insert row-by-row, passing the data tuple separately ---
        for row in tqdm(df.itertuples(index=False, name=None), desc=f"Processing table {table}"):
            cursor.execute(insert_sql, row)

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    load_csv_folder_to_mysql(
        directory="archive",
        host="localhost",
        user="root",
        password=Passwords.mysql,
        database="store"
    )
