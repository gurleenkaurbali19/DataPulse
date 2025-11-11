from utils.database_connection import create_connection
from mysql.connector import Error

def retrieve_table(table_name):
    """
    Fetches all rows from a given table in DataPulse_db.
    
    Args:
        table_name (str): Name of the table to fetch data from.
    
    Returns:
        list of dict: Rows from the table as dictionaries.
        False if there is an error or connection issue.
    """
    conn = create_connection()
    if not conn:
        print("Can't connect to the database!!")
        return False
    cursor = conn.cursor(dictionary=True)  # fetch rows as dicts
    try:
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        data = cursor.fetchall()
        print(f"Fetched data from {table_name} table!!")
        return data
    except Error as e:
        print(f"Error fetching data from {table_name}: {e}")
        return False
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("Connection closed.")
