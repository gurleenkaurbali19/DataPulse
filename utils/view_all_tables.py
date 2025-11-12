from utils.database_connection import create_connection
from mysql.connector import Error

def view_all_tables():
    """
    Fetches and displays all tables and their data from DataPulse_db.
    """
    conn = create_connection()
    if not conn:
        print("Failed to connect to database.")
        return

    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()

        if not tables:
            print("No tables found in the database.")
            return

        print("\nAVAILABLE TABLES:")
        for t in tables:
            print(" -", list(t.values())[0])

        print("\n=========================\n")

        for t in tables:
            table_name = list(t.values())[0]
            print(f"TABLE: {table_name}")
            print("-" * 50)
            try:
                cursor.execute(f"SELECT * FROM {table_name}")
                rows = cursor.fetchall()

                if not rows:
                    print("(No data found)\n")
                else:
                    for row in rows:
                        print(row)
                    print("\nTotal rows:", len(rows))
                print("\n" + "=" * 50 + "\n")
            except Error as e:
                print(f"⚠️ Error fetching data from {table_name}: {e}\n")

    except Error as e:
        print(f" Error while fetching tables: {e}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("Connection closed.")

if __name__ == "__main__":
    view_all_tables()
