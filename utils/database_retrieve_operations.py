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
def retrieve_all_customers():
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        query = """
            SELECT customer_id, customer_name, 'RAW' AS source FROM customers_raw
            UNION ALL
            SELECT customer_id, customer_name, 'MAIN' AS source FROM customers
        """
        cursor.execute(query)
        return cursor.fetchall()
    except:
        return []
    finally:
        cursor.close()
        conn.close()
def retrieve_all_products():
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        query = """
            SELECT 
                product_id,
                product_name,
                category,
                selling_price,
                cost_price,
                stock,
                'RAW' AS source 
            FROM products_raw
            
            UNION ALL
            
            SELECT 
                product_id,
                product_name,
                category,
                selling_price,
                cost_price,
                stock,
                'MAIN' AS source 
            FROM products
        """
        cursor.execute(query)
        return cursor.fetchall()

    except Exception as e:
        print("Error in retrieve_all_products:", e)
        return []

    finally:
        cursor.close()
        conn.close()

def retrieve_all_orders():
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        query = """
            SELECT order_id, customer_id, product_id, quantity, 'RAW' AS source 
            FROM orders_raw
            UNION ALL
            SELECT order_id, customer_id, product_id, quantity, 'MAIN' AS source 
            FROM orders
        """
        cursor.execute(query)
        return cursor.fetchall()
    except:
        return []
    finally:
        cursor.close()
        conn.close()
