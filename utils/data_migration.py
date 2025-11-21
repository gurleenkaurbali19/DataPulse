from utils.database_connection import create_connection
from mysql.connector import Error
import pandas as pd

def migrate_customer():
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)
    inserted_count = 0
    skipped_count = 0

    try:
        # Load raw customers
        cursor.execute("SELECT * FROM customers_raw")
        customer_raw_df = pd.DataFrame(cursor.fetchall())

        # Load main customers
        cursor.execute("SELECT * FROM customers")
        customer_main_df = pd.DataFrame(cursor.fetchall())

        # Case 1: Main table empty → insert all
        if customer_main_df.empty:
            for idx, row in customer_raw_df.iterrows():
                cursor.execute(
                    "INSERT INTO customers (customer_name, email, phone, city) VALUES (%s, %s, %s, %s)",
                    (row['customer_name'], row['email'], row['phone'], row['city'])
                )
                main_id = cursor.lastrowid   #last row id (primary key column)
                # Update mapping table
                cursor.execute(
                    "INSERT INTO customer_mapping (raw_customer_id, main_customer_id) VALUES (%s, %s)",
                    (row['customer_id'], main_id)
                )
                inserted_count += 1

        # Case 2: Main table has existing customers
        else:
            # set of existing phone numbers for quick lookup
            existing_phones = set(customer_main_df['phone'])

            for idx, row in customer_raw_df.iterrows():
                phone = row['phone']
                raw_id = row['customer_id']

                # Skip if phone already exists
                if phone in existing_phones:
                    skipped_count += 1
                    continue

                # Insert new customer
                cursor.execute(
                    "INSERT INTO customers (customer_name, email, phone, city) VALUES (%s, %s, %s, %s)",
                    (row['customer_name'], row['email'], phone, row['city'])
                )
                main_id = cursor.lastrowid
                existing_phones.add(phone)
                inserted_count += 1

                # Update mapping table
                cursor.execute(
                    "INSERT INTO customer_mapping (raw_customer_id, main_customer_id) VALUES (%s, %s)",
                    (raw_id, main_id)
                )

        conn.commit()
        return {"status": "success", "inserted": inserted_count, "skipped": skipped_count}

    except Error as e:
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()

def migrate_product():
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)
    inserted_count = 0
    skipped_count = 0

    try:
        # Load raw products
        cursor.execute("SELECT * FROM products_raw")
        product_raw_df = pd.DataFrame(cursor.fetchall())

        # Load main products
        cursor.execute("SELECT * FROM products")
        product_main_df = pd.DataFrame(cursor.fetchall())

        # Case 1: Main table empty → insert all
        if product_main_df.empty:
            for idx, row in product_raw_df.iterrows():
                cursor.execute(
                    "INSERT INTO products (product_name, category, selling_price, cost_price, stock) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (row['product_name'], row['category'], row['selling_price'], row['cost_price'], row['stock'])
                )
                main_id = cursor.lastrowid

                # Update mapping table
                cursor.execute(
                    "INSERT INTO product_mapping (raw_product_id, main_product_id) VALUES (%s, %s)",
                    (row['product_id'], main_id)
                )
                inserted_count += 1

        # Case 2: Main table has existing products
        else:
            # Set of existing product names
            existing_products = set(product_main_df['product_name'])

            for idx, row in product_raw_df.iterrows():
                product_name = row['product_name']
                raw_id = row['product_id']

                # Skip if product already exists
                if product_name in existing_products:
                    skipped_count += 1
                    continue

                # Insert new product
                cursor.execute(
                    "INSERT INTO products (product_name, category, selling_price, cost_price, stock) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (product_name, row['category'], row['selling_price'], row['cost_price'], row['stock'])
                )
                main_id = cursor.lastrowid
                existing_products.add(product_name)
                inserted_count += 1

                # Insert mapping
                cursor.execute(
                    "INSERT INTO product_mapping (raw_product_id, main_product_id) VALUES (%s, %s)",
                    (raw_id, main_id)
                )

        conn.commit()
        return {"status": "success", "inserted": inserted_count, "skipped": skipped_count}

    except Error as e:
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()

def migrate_order():
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        # Loading raw orders
        cursor.execute("SELECT * FROM orders_raw")
        order_raw = pd.DataFrame(cursor.fetchall())

        # Loading mapping tables
        cursor.execute("SELECT * FROM customer_mapping")
        customer_map = pd.DataFrame(cursor.fetchall())

        cursor.execute("SELECT * FROM product_mapping")
        product_map = pd.DataFrame(cursor.fetchall())

        # Converting mapping rows to dictionaries for fast lookup
        customer_lookup = dict(zip(customer_map['raw_customer_id'], customer_map['main_customer_id']))
        product_lookup = dict(zip(product_map['raw_product_id'], product_map['main_product_id']))

        for idx, row in order_raw.iterrows():

            raw_order_id = row['order_id']         # raw PK
            raw_cid = row['customer_id']
            raw_pid = row['product_id']

            # Fetch mapped main IDs
            c_main_id = customer_lookup.get(raw_cid)
            p_main_id = product_lookup.get(raw_pid)

            # If mapping missing → skip
            if c_main_id is None or p_main_id is None:
                continue

            # Insert into main orders table (duplicate orders allowed)
            cursor.execute(
                """INSERT INTO orders 
                   (customer_id, product_id, quantity, order_status, payment_method)
                   VALUES (%s, %s, %s, %s, %s)""",
                (c_main_id, p_main_id, row['quantity'], row['order_status'], row['payment_method'])
            )

            # Get new main order ID
            main_order_id = cursor.lastrowid

            # Inserting mapping into order_mapping
            cursor.execute(
                """INSERT INTO order_mapping (raw_order_id, main_order_id)
                   VALUES (%s, %s)""",
                (raw_order_id, main_order_id)
            )

        conn.commit()
        return {"status": "success", "message": "Orders migrated & mapping updated"}

    except Error as e:
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()

def migrate_sales():
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        # Loading raw sales
        cursor.execute("SELECT * FROM sales_raw")
        sales_raw = pd.DataFrame(cursor.fetchall())

        # Loading mapping table (order → main order)
        cursor.execute("SELECT * FROM order_mapping")
        order_map = pd.DataFrame(cursor.fetchall())

        # lookup dictionary: raw_order_id → main_order_id
        order_lookup = dict(zip(order_map['raw_order_id'], order_map['main_order_id']))

        for idx, row in sales_raw.iterrows():

            raw_sale_id = row['sale_id']    # raw PK
            raw_oid = row['order_id']       # raw order_id

            # Getting mapped main order_id
            main_order_id = order_lookup.get(raw_oid)

            # Skip if mapping missing
            if main_order_id is None:
                continue

            # Inserting into main sales table (duplicates allowed)
            cursor.execute(
                """INSERT INTO sales 
                   (order_id, sale_amount, profit, region)
                   VALUES (%s, %s, %s, %s)""",
                (main_order_id, row['sale_amount'], row['profit'], row['region'])
            )

            # Get newly inserted sale_id (main table)
            main_sale_id = cursor.lastrowid

            # Insert into sale_mapping
            cursor.execute(
                """INSERT INTO sale_mapping (raw_sale_id, main_sale_id)
                   VALUES (%s, %s)""",
                (raw_sale_id, main_sale_id)
            )

        conn.commit()
        return {"status": "success", "message": "Sales migrated & mapping updated"}

    except Error as e:
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()

def migrate_all():
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)

    summary = {
        "customer_migration": None,
        "product_migration": None,
        "order_migration": None,
        "sales_migration": None
    }

    try:
        # -------------------------
        # 1. CUSTOMERS
        # -------------------------
        cursor.execute("SELECT COUNT(*) AS cnt FROM customers_raw")
        if cursor.fetchone()["cnt"] > 0:
            summary["customer_migration"] = migrate_customer()
        else:
            summary["customer_migration"] = {"status": "skipped", "reason": "customers_raw is empty"}

        # -------------------------
        # 2. PRODUCTS
        # -------------------------
        cursor.execute("SELECT COUNT(*) AS cnt FROM products_raw")
        if cursor.fetchone()["cnt"] > 0:
            summary["product_migration"] = migrate_product()
        else:
            summary["product_migration"] = {"status": "skipped", "reason": "products_raw is empty"}

        # -------------------------
        # 3. ORDERS
        # -------------------------
        cursor.execute("SELECT COUNT(*) AS cnt FROM orders_raw")
        if cursor.fetchone()["cnt"] > 0:
            summary["order_migration"] = migrate_order()
        else:
            summary["order_migration"] = {"status": "skipped", "reason": "orders_raw is empty"}

        # -------------------------
        # 4. SALES
        # -------------------------
        cursor.execute("SELECT COUNT(*) AS cnt FROM sales_raw")
        if cursor.fetchone()["cnt"] > 0:
            summary["sales_migration"] = migrate_sales()
        else:
            summary["sales_migration"] = {"status": "skipped", "reason": "sales_raw is empty"}

        return summary

    except Exception as e:
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()
