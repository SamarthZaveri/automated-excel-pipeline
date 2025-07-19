import sqlite3

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connected to SQLite database: {db_file}")
    except sqlite3.Error as e:
        print(e)
    return conn

def create_table(conn):
    try:
        sql_create_table = """
        CREATE TABLE IF NOT EXISTS data_filter (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company TEXT NOT NULL,
            col_name TEXT NOT NULL,
            raw_value TEXT NOT NULL,
            norm_value TEXT NOT NULL
        );
        """
        cursor = conn.cursor()
        cursor.execute(sql_create_table)
        print("Table 'data_filter' is ready.")
    except sqlite3.Error as e:
        print(e)

def insert_data_filter(conn, company, col_name, raw_value, norm_value):
    try:
        sql_insert = """
        INSERT INTO data_filter (company, col_name, raw_value, norm_value)
        VALUES (?, ?, ?, ?)
        """
        cursor = conn.cursor()
        cursor.execute(sql_insert, (company, col_name, raw_value, norm_value))
        conn.commit()
        print(f"Inserted: {company}, {col_name} → {raw_value} → {norm_value}")
    except sqlite3.Error as e:
        print(e)

def select_all_filters(conn):
    try:
        sql_select = "SELECT * FROM data_filter"
        cursor = conn.cursor()
        cursor.execute(sql_select)
        rows = cursor.fetchall()
        print("All entries in 'data_filter':")
        for row in rows:
            print(row)
    except sqlite3.Error as e:
        print(e)

def main():
    database = "filter.db"
    conn = create_connection(database)
    if conn:
        create_table(conn)
        # Sample insert 
        
        select_all_filters(conn)
        conn.close()
    else:
        print("Error! Cannot create the database connection.")

if __name__ == '__main__':
    main()
