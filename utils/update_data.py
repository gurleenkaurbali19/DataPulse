from mysql.connector import Error
from utils.database_connection import create_connection
import pandas as pd


# ----------------------------
# RAW TABLES UPDATION FUNCTIONS
# ----------------------------

def update_customer_raw(c_id, c_name, c_email, c_phone, c_city):   # Everything can be updated accept the id and created_at
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            UPDATE customers_raw 
            SET customer_name=%s, email=%s, phone=%s, city=%s
            WHERE customer_id=%s
            """,
            (c_name, c_email, c_phone, c_city, c_id),
        )
        conn.commit()
        return {"status": "success", "message": "Record updated successfully"}
    except Error as e:
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close()
        conn.close()

def update_product_raw(p_id, p_name, p_category, p_sp, p_cp, p_stock):
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        cursor.execute(
            """
            UPDATE products_raw 
            SET product_name=%s, category=%s, selling_price=%s, cost_price=%s, stock=%s
            WHERE product_id=%s
            """,
            (p_name, p_category, p_sp, p_cp, p_stock, p_id),
        )

        # Fetching orders for this product
        cursor.execute(
            "SELECT order_id, quantity FROM orders_raw WHERE product_id=%s",
            (p_id,)
        )
        order = pd.DataFrame(cursor.fetchall())

        # If orders exist, update sales_raw accordingly
        if not order.empty:
            for idx, row in order.iterrows():
                s_amt = row['quantity'] * p_sp
                s_profit = (p_sp - p_cp) * row['quantity']

                cursor.execute(
                    """
                    UPDATE sales_raw 
                    SET sale_amount=%s, profit=%s 
                    WHERE order_id=%s
                    """,
                    (s_amt, s_profit, row['order_id'])
                )

        conn.commit()
        return {"status": "success", "message": "Record updated successfully"}

    except Exception as e:
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()

def update_order_raw(o_id, o_quantity, o_status, o_payment_method):
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        cursor.execute(
            """
            UPDATE orders_raw 
            SET quantity=%s, order_status=%s, payment_method=%s
            WHERE order_id=%s
            """,
            (o_quantity, o_status, o_payment_method, o_id),
        )
        
        # Fetching updated order
        cursor.execute("SELECT * FROM orders_raw WHERE order_id=%s", (o_id,))
        order = pd.DataFrame(cursor.fetchall())

        if order.empty:
            return {"status": "error", "message": "Order not found"}

        product_id = order.loc[0, "product_id"]

        # Fetching the corresponding product
        cursor.execute("SELECT * FROM products_raw WHERE product_id=%s", (product_id,))
        product = pd.DataFrame(cursor.fetchall())

        if product.empty:
            return {"status": "error", "message": "Product not found"}

        sp = product.loc[0, "selling_price"]
        cp = product.loc[0, "cost_price"]

        # sale amount & profit
        s_amt = o_quantity * sp
        s_profit = (sp - cp) * o_quantity

        # Updating sales_raw
        cursor.execute(
            """UPDATE sales_raw SET sale_amount=%s, profit=%s WHERE order_id=%s""",
            (s_amt, s_profit, o_id)
        )

        conn.commit()
        return {"status": "success", "message": "Record updated successfully"}

    except Exception as e:
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()

def update_sale_raw(s_id, s_region):
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """UPDATE sales_raw SET region=%s WHERE sale_id=%s""",
            (s_region, s_id)
        )
        conn.commit()

        return {"status": "success", "message": "Record updated successfully"}

    except Exception as e:
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()



# ----------------------------
# MAIN TABLES UPDATION FUNCTIONS
# ----------------------------

def update_customer_main(c_id, c_name, c_email, c_city):   # Everything can be updated accept the id and created_at and phone
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            UPDATE customers
            SET customer_name=%s, email=%s, city=%s
            WHERE customer_id=%s
            """,
            (c_name, c_email, c_city, c_id),
        )
        conn.commit()
        return {"status": "success", "message": "Record updated successfully"}
    except Error as e:
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close()
        conn.close()

def update_product_main(p_id,p_sp, p_cp, p_stock):
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        cursor.execute(
            """
            UPDATE products 
            SET selling_price=%s, cost_price=%s, stock=%s
            WHERE product_id=%s
            """,
            (p_sp, p_cp, p_stock, p_id),
        )

        # Fetching orders for this product
        cursor.execute(
            "SELECT order_id, quantity FROM orders WHERE product_id=%s",
            (p_id,)
        )
        order = pd.DataFrame(cursor.fetchall())

        # If orders exist, update sales accordingly
        if not order.empty:
            for idx, row in order.iterrows():
                s_amt = row['quantity'] * p_sp
                s_profit = (p_sp - p_cp) * row['quantity']

                cursor.execute(
                    """
                    UPDATE sales
                    SET sale_amount=%s, profit=%s 
                    WHERE order_id=%s
                    """,
                    (s_amt, s_profit, row['order_id'])
                )

        conn.commit()
        return {"status": "success", "message": "Record updated successfully"}

    except Exception as e:
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()

def update_order_main(o_id, o_status, o_payment_method):
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        cursor.execute(
            """
            UPDATE orders
            SET order_status=%s, payment_method=%s
            WHERE order_id=%s
            """,
            (o_status, o_payment_method, o_id),
        )
        

        conn.commit()
        return {"status": "success", "message": "Record updated successfully"}

    except Exception as e:
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()

def update_sale_main(s_id, s_region):
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """UPDATE sales SET region=%s WHERE sale_id=%s""",
            (s_region, s_id)
        )
        conn.commit()

        return {"status": "success", "message": "Record updated successfully"}

    except Exception as e:
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()