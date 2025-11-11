from utils.database_connection import create_connection

def test_db_connection():
    conn = create_connection()
    if conn:
        print("Database connection successful!")

        cursor = conn.cursor()
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()

        print("\nTables in DataPulse_db:")
        if tables:
            for (table_name,) in tables:
                print(f"   • {table_name}")
        else:
            print("No tables found in the database.")

        cursor.close()
        conn.close()
        print("\nConnection closed.")
    else:
        print("Failed to connect to the database.")

if __name__ == "__main__":
    test_db_connection()
