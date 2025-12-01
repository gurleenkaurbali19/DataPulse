# utils/update_data_operations.py
from mysql.connector import Error
from utils.database_connection import create_connection
import pandas as pd
import numpy as np

# ----------------------------
# Helper: convert numpy/pandas types to native Python types
# ----------------------------
def to_python(val):
    """Convert numpy/pandas numeric types and pd.NA to native Python types."""
    if val is None:
        return None
    # pandas NA
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return float(val)
    # regular python int/float/str remain
    return val

# ----------------------------
# RAW TABLE UPDATION FUNCTIONS
# ----------------------------

def update_customer_raw(c_id, c_name, c_email, c_phone, c_city):
    # convert inputs to native Python types
    c_id = to_python(c_id)
    c_name = None if c_name is None else str(c_name).strip()
    c_email = None if c_email is None or str(c_email).strip() == "" else str(c_email).strip()
    c_phone = None if c_phone is None else str(c_phone).strip()
    c_city = None if c_city is None or str(c_city).strip() == "" else str(c_city).strip()

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
        return {"status": "success", "message": "Customer (raw) updated successfully"}
    except Error as e:
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close()
        conn.close()

def update_product_raw(p_id, p_name, p_category, p_sp, p_cp, p_stock):
    p_id = to_python(p_id)
    p_name = None if p_name is None else str(p_name).strip()
    p_category = None if p_category is None else str(p_category).strip()
    p_sp = to_python(p_sp)
    p_cp = to_python(p_cp)
    p_stock = to_python(p_stock)

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

        # Fetching orders for this product (use native p_id)
        cursor.execute(
            "SELECT order_id, quantity FROM orders_raw WHERE product_id=%s",
            (p_id,),
        )
        orders = cursor.fetchall()
        if orders:
            for r in orders:
                order_id = to_python(r.get("order_id"))
                qty = to_python(r.get("quantity")) or 0

                # ensure numeric math uses Python floats/ints
                sp = float(p_sp) if p_sp is not None else 0.0
                cp = float(p_cp) if p_cp is not None else 0.0
                s_amt = float(qty) * sp
                s_profit = float(qty) * (sp - cp)

                cursor.execute(
                    """
                    UPDATE sales_raw
                    SET sale_amount=%s, profit=%s
                    WHERE order_id=%s
                    """,
                    (s_amt, s_profit, order_id),
                )

        conn.commit()
        return {"status": "success", "message": "Product (raw) updated successfully"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close()
        conn.close()

def update_order_raw(o_id, o_quantity, o_status, o_payment_method):
    o_id = to_python(o_id)
    o_quantity = to_python(o_quantity) or 0
    o_status = None if o_status is None else str(o_status).strip()
    o_payment_method = None if o_payment_method is None else str(o_payment_method).strip()

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

        # Re-fetch order to compute sale values
        cursor.execute("SELECT * FROM orders_raw WHERE order_id=%s", (o_id,))
        order_rows = cursor.fetchall()
        if not order_rows:
            conn.commit()
            return {"status": "error", "message": "Order not found (raw)"}
        order_row = order_rows[0]
        product_id = to_python(order_row.get("product_id"))

        # Fetch product for prices
        cursor.execute("SELECT selling_price, cost_price FROM products_raw WHERE product_id=%s", (product_id,))
        prod_rows = cursor.fetchall()
        if not prod_rows:
            conn.commit()
            return {"status": "error", "message": "Product not found (raw) for recalculation"}

        sp = to_python(prod_rows[0].get("selling_price")) or 0.0
        cp = to_python(prod_rows[0].get("cost_price")) or 0.0

        qty = to_python(o_quantity) or 0
        s_amt = float(qty) * float(sp)
        s_profit = float(qty) * (float(sp) - float(cp))

        cursor.execute(
            """UPDATE sales_raw SET sale_amount=%s, profit=%s WHERE order_id=%s""",
            (s_amt, s_profit, o_id),
        )

        conn.commit()
        return {"status": "success", "message": "Order (raw) updated successfully"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close()
        conn.close()

def update_sale_raw(s_id, s_region):
    s_id = to_python(s_id)
    s_region = None if s_region is None or str(s_region).strip() == "" else str(s_region).strip()

    conn = create_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            "UPDATE sales_raw SET region=%s WHERE sale_id=%s",
            (s_region, s_id),
        )
        conn.commit()
        return {"status": "success", "message": "Sale (raw) updated successfully"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close()
        conn.close()

# ----------------------------
# MAIN TABLES UPDATION FUNCTIONS
# ----------------------------

def update_customer_main(c_id, c_name, c_email, c_city):
    c_id = to_python(c_id)
    c_name = None if c_name is None else str(c_name).strip()
    c_email = None if c_email is None else str(c_email).strip()
    c_city = None if c_city is None else str(c_city).strip()

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
        return {"status": "success", "message": "Customer updated successfully"}
    except Error as e:
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close()
        conn.close()

def update_product_main(p_id, p_sp, p_cp, p_stock):
    p_id = to_python(p_id)
    p_sp = to_python(p_sp)
    p_cp = to_python(p_cp)
    p_stock = to_python(p_stock)

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

        # update dependent sales rows
        cursor.execute("SELECT order_id, quantity FROM orders WHERE product_id=%s", (p_id,))
        orders = cursor.fetchall()
        if orders:
            for r in orders:
                order_id = to_python(r.get("order_id"))
                qty = to_python(r.get("quantity")) or 0
                sp = float(p_sp) if p_sp is not None else 0.0
                cp = float(p_cp) if p_cp is not None else 0.0

                s_amt = float(qty) * sp
                s_profit = float(qty) * (sp - cp)

                cursor.execute(
                    "UPDATE sales SET sale_amount=%s, profit=%s WHERE order_id=%s",
                    (s_amt, s_profit, order_id),
                )

        conn.commit()
        return {"status": "success", "message": "Product updated successfully"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close()
        conn.close()

def update_order_main(o_id, o_status, o_payment_method):
    o_id = to_python(o_id)
    o_status = None if o_status is None else str(o_status).strip()
    o_payment_method = None if o_payment_method is None else str(o_payment_method).strip()

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
        return {"status": "success", "message": "Order updated successfully"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close()
        conn.close()

def update_sale_main(s_id, s_region):
    s_id = to_python(s_id)
    s_region = None if s_region is None or str(s_region).strip() == "" else str(s_region).strip()
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("UPDATE sales SET region=%s WHERE sale_id=%s", (s_region, s_id))
        conn.commit()
        return {"status": "success", "message": "Sale updated successfully"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        cursor.close()
        conn.close()
