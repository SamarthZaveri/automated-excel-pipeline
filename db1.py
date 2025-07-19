import sqlite3

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"✅ Connected to SQLite database: {db_file}")
    except sqlite3.Error as e:
        print(f"❌ Error: {e}")
    return conn

def create_table(conn):
    try:
        sql_create_table = """
        CREATE TABLE IF NOT EXISTS person (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            col_name TEXT NOT NULL,
            norm_col_name TEXT NOT NULL
        );
        """
        cursor = conn.cursor()
        cursor.execute(sql_create_table)

        # Enforce uniqueness to prevent future duplicates
        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_mapping
            ON person (name, col_name, norm_col_name);
        """)

        conn.commit()
        print("✅ Table 'person' and unique index are ready.")
    except sqlite3.Error as e:
        print(f"❌ Error creating table: {e}")

def remove_duplicates(conn):
    try:
        sql = """
        DELETE FROM person
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM person
            GROUP BY name, col_name, norm_col_name
        );
        """
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        print("🧹 Duplicate rows removed (only first instance kept).")
    except sqlite3.Error as e:
        print(f"❌ Error removing duplicates: {e}")

def insert_person(conn, name, col_name, norm_col_name):
    try:
        sql_insert = "INSERT OR IGNORE INTO person (name, col_name, norm_col_name) VALUES (?, ?, ?)"
        cursor = conn.cursor()
        cursor.execute(sql_insert, (name, col_name, norm_col_name))
        conn.commit()
        print(f"➕ Inserted (or skipped if duplicate): {name}, {col_name}, {norm_col_name}")
    except sqlite3.Error as e:
        print(f"❌ Error inserting person: {e}")

def select_all_persons(conn):
    try:
        sql_select = "SELECT * FROM person"
        cursor = conn.cursor()
        cursor.execute(sql_select)
        rows = cursor.fetchall()
        print("📋 All entries in 'person' table:")
        for row in rows:
            print(row)
    except sqlite3.Error as e:
        print(f"❌ Error reading table: {e}")

def main():
    database = "row_clean.db"
    conn = create_connection(database)
    if conn:
        create_table(conn)
        # Insert data
        
        select_all_persons(conn)
        conn.close()
    else:
        print("Error! Cannot create the database connection.")

if __name__ == '__main__':
    main()
