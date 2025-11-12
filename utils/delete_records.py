from utils.database_connection import create_connection

def delete_rows_from_table(table_name, condition=None):
    """
    Delete rows from a specified table based on an optional condition.
    If no condition is provided, all rows are deleted (use with caution).
    """
    conn = create_connection()
    if not conn:
        print("Database connection failed!")
        return
    
    cursor = conn.cursor()
    try:
        if condition:
            query = f"DELETE FROM {table_name} WHERE {condition}"
        else:
            query = f"DELETE FROM {table_name}"  # Delete all rows

        print(f"WARNING: You are about to run:\n{query}")
        confirm = input("Type 'yes' to confirm: ").strip().lower()
        if confirm != 'yes':
            print("Operation cancelled.")
            return
        
        cursor.execute(query)
        conn.commit()
        print(f"Successfully deleted rows from {table_name}.")
    except Exception as e:
        print(f"Error during delete operation: {e}")
    finally:
        cursor.close()
        conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    tables = ["customers_raw", "products_raw", "orders_raw", "sales_raw"]
    print("Tables:", tables)
    table_name = input("Enter table name to delete from: ").strip()
    if table_name not in tables:
        print("Invalid table name.")
    else:
        choice = input("Delete (1) all rows or (2) by condition? Enter 1 or 2: ").strip()
        condition = None
        if choice == '2':
            condition = input("Enter SQL WHERE condition (e.g. customer_id = 5): ").strip()
            if not condition:
                print("No condition entered, cancelling delete.")
                exit()

        delete_rows_from_table(table_name, condition)
