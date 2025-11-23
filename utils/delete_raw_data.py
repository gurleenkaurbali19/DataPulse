from utils.database_connection import create_connection
from mysql.connector import Error
import pandas as pd

def del_customer():
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)

    not_migrated_ids = [] 
    deleted_count = 0

    try:
        # Raw table guaranteed to have data (checked by master function)
        cursor.execute("SELECT customer_id FROM customers_raw")
        customer_raw = pd.DataFrame(cursor.fetchall())

        # Load mapping table
        cursor.execute("SELECT raw_customer_id FROM customer_mapping")
        cust_mapping = pd.DataFrame(cursor.fetchall())

        migrated_raw_ids = set(cust_mapping['raw_customer_id']) if not cust_mapping.empty else set()

        # Determine IDs to delete: intersection of raw table and migrated IDs
        ids_to_delete = migrated_raw_ids.intersection(set(customer_raw['customer_id']))

        # Not migrated IDs for reporting
        not_migrated_ids = [rid for rid in customer_raw['customer_id'] if rid not in migrated_raw_ids]

        if ids_to_delete:
            # Bulk delete using IN clause
            format_strings = ','.join(['%s'] * len(ids_to_delete))
            query = f"DELETE FROM customers_raw WHERE customer_id IN ({format_strings})"
            cursor.execute(query, tuple(ids_to_delete))
            deleted_count = cursor.rowcount

        conn.commit()

        return {
            "status": "success",
            "deleted_count": deleted_count,
            "not_migrated_count": len(not_migrated_ids),
            "not_migrated_ids": not_migrated_ids
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()

def del_product():
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)

    not_migrated_ids = []
    deleted_count = 0

    try:
        # Raw table guaranteed to have data (checked by master function)
        cursor.execute("SELECT product_id FROM products_raw")
        product_raw = pd.DataFrame(cursor.fetchall())

        # Loading mapping table
        cursor.execute("SELECT raw_product_id FROM product_mapping")
        prod_mapping = pd.DataFrame(cursor.fetchall())

        migrated_raw_ids = set(prod_mapping['raw_product_id']) if not prod_mapping.empty else set()

        # Determine product_ids to delete
        raw_ids = set(product_raw['product_id'])
        ids_to_delete = migrated_raw_ids.intersection(raw_ids)

        # Not migrated
        not_migrated_ids = list(raw_ids - migrated_raw_ids)

        # Bulk delete
        if ids_to_delete:
            format_strings = ','.join(['%s'] * len(ids_to_delete))
            query = f"DELETE FROM products_raw WHERE product_id IN ({format_strings})"
            cursor.execute(query, tuple(ids_to_delete))
            deleted_count = cursor.rowcount

        conn.commit()

        return {
            "status": "success",
            "deleted_count": deleted_count,
            "not_migrated_count": len(not_migrated_ids),
            "not_migrated_ids": not_migrated_ids
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()

def del_order():
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)

    not_migrated_ids = []
    deleted_count = 0

    try:
        cursor.execute("SELECT order_id FROM orders_raw")
        order_raw = pd.DataFrame(cursor.fetchall())

        cursor.execute("SELECT raw_order_id FROM order_mapping")
        order_mapping = pd.DataFrame(cursor.fetchall())

        migrated_raw_ids = set(order_mapping['raw_order_id']) if not order_mapping.empty else set()

        raw_ids = set(order_raw['order_id'])
        ids_to_delete = migrated_raw_ids.intersection(raw_ids)

        not_migrated_ids = list(raw_ids - migrated_raw_ids)

        if ids_to_delete:
            format_strings = ",".join(["%s"] * len(ids_to_delete))
            query = f"DELETE FROM orders_raw WHERE order_id IN ({format_strings})"
            cursor.execute(query, tuple(ids_to_delete))
            deleted_count = cursor.rowcount

        conn.commit()

        return {
            "status": "success",
            "deleted_count": deleted_count,
            "not_migrated_count": len(not_migrated_ids),
            "not_migrated_ids": not_migrated_ids
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()

def del_sale():
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)

    not_migrated_ids = []
    deleted_count = 0

    try:
        cursor.execute("SELECT sale_id FROM sales_raw")
        sales_raw = pd.DataFrame(cursor.fetchall())

        cursor.execute("SELECT raw_sale_id FROM sale_mapping")
        sale_mapping = pd.DataFrame(cursor.fetchall())

        migrated_raw_ids = set(sale_mapping['raw_sale_id']) if not sale_mapping.empty else set()

        raw_ids = set(sales_raw['sale_id'])
        ids_to_delete = migrated_raw_ids.intersection(raw_ids)

        not_migrated_ids = list(raw_ids - migrated_raw_ids)

        if ids_to_delete:
            format_strings = ",".join(["%s"] * len(ids_to_delete))
            query = f"DELETE FROM sales_raw WHERE sale_id IN ({format_strings})"
            cursor.execute(query, tuple(ids_to_delete))
            deleted_count = cursor.rowcount

        conn.commit()

        return {
            "status": "success",
            "deleted_count": deleted_count,
            "not_migrated_count": len(not_migrated_ids),
            "not_migrated_ids": not_migrated_ids
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()
    
def delete_all_raw():
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)

    results = {}

    try:
        # CUSTOMER RAW
        cursor.execute("SELECT COUNT(*) AS cnt FROM customers_raw")
        if cursor.fetchone()['cnt'] > 0:
            results['customer'] = del_customer()
        else:
            results['customer'] = {"status": "skipped", "reason": "customers_raw empty"}

        # PRODUCT RAW
        cursor.execute("SELECT COUNT(*) AS cnt FROM products_raw")
        if cursor.fetchone()['cnt'] > 0:
            results['product'] = del_product()
        else:
            results['product'] = {"status": "skipped", "reason": "products_raw empty"}

        # ORDER RAW
        cursor.execute("SELECT COUNT(*) AS cnt FROM orders_raw")
        if cursor.fetchone()['cnt'] > 0:
            results['order'] = del_order()
        else:
            results['order'] = {"status": "skipped", "reason": "orders_raw empty"}

        # SALES RAW
        cursor.execute("SELECT COUNT(*) AS cnt FROM sales_raw")
        if cursor.fetchone()['cnt'] > 0:
            results['sale'] = del_sale()
        else:
            results['sale'] = {"status": "skipped", "reason": "sales_raw empty"}

        return results

    except Exception as e:
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()
