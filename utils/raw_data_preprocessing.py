import pandas as pd
from mysql.connector import Error
from utils.database_connection import create_connection


# =====================================================
# PREPROCESS CUSTOMERS
# =====================================================
def preprocess_raw_customer():
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        # STEP 1: Load RAW tables
        cursor.execute("SELECT * FROM customers_raw")
        customer_df = pd.DataFrame(cursor.fetchall())

        cursor.execute("SELECT * FROM orders_raw")
        order_df = pd.DataFrame(cursor.fetchall())

        cursor.execute("SELECT * FROM sales_raw")
        sales_df = pd.DataFrame(cursor.fetchall())

        # -------------------------------------------------
        # STEP 2: Fill missing CITY
        # -------------------------------------------------
        empty_city = customer_df[customer_df["city"] == ""]

        if order_df.empty or sales_df.empty:
            # No inference possible -> fill Unknown
            for _, row in empty_city.iterrows():
                cursor.execute(
                    "UPDATE customers_raw SET city=%s WHERE customer_id=%s",
                    ("Unknown", row["customer_id"])
                )
            conn.commit()

        else:
            # Infer city from sales.region
            for _, row in empty_city.iterrows():
                cust_id = row["customer_id"]

                cust_orders = order_df[order_df["customer_id"] == cust_id]

                if cust_orders.empty:
                    final_city = "Unknown"
                else:
                    order_ids = cust_orders["order_id"].tolist()
                    related_sales = sales_df[
                        (sales_df["order_id"].isin(order_ids)) &
                        (sales_df["region"] != "")
                    ]

                    final_city = related_sales.iloc[0]["region"] if not related_sales.empty else "Unknown"

                cursor.execute(
                    "UPDATE customers_raw SET city=%s WHERE customer_id=%s",
                    (final_city, cust_id)
                )

            conn.commit()

        # -------------------------------------------------
        # STEP 3: Fill missing EMAIL
        # -------------------------------------------------
        empty_email = customer_df[customer_df["email"] == ""]
        for _, row in empty_email.iterrows():
            cursor.execute(
                "UPDATE customers_raw SET email=%s WHERE customer_id=%s",
                ("Unknown", row["customer_id"])
            )
        conn.commit()

        # -------------------------------------------------
        # STEP 4: Cleanup (trim, lowercase, remove extra spaces)
        # -------------------------------------------------
        cleanup_queries = [
            """
            UPDATE customers_raw
            SET customer_name = TRIM(customer_name),
                email = TRIM(email),
                city = TRIM(city)
            """,

            "UPDATE customers_raw SET email = LOWER(email)",

            """
            UPDATE customers_raw
            SET customer_name = REGEXP_REPLACE(customer_name, '\\s+', ' ')
            """
        ]

        for q in cleanup_queries:
            cursor.execute(q)
        conn.commit()

        # -------------------------------------------------
        # STEP 5: Title Case name + city
        # -------------------------------------------------
        cursor.execute("SELECT customer_id, customer_name, city FROM customers_raw")
        rows = cursor.fetchall()

        for r in rows:
            clean_name = r["customer_name"].title() if r["customer_name"] else ""
            clean_city = r["city"].title() if r["city"] else ""

            cursor.execute(
                """
                UPDATE customers_raw
                SET customer_name=%s, city=%s
                WHERE customer_id=%s
                """,
                (clean_name, clean_city, r["customer_id"])
            )

        conn.commit()
        return True

    except Error as e:
        print(f"Error during preprocessing customers: {e}")
        return False

    finally:
        cursor.close()
        conn.close()



# =====================================================
# PREPROCESS SALES
# =====================================================
def preprocess_raw_sale():
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("SELECT * FROM customers_raw")
        customers_df = pd.DataFrame(cursor.fetchall())

        cursor.execute("SELECT * FROM orders_raw")
        orders_df = pd.DataFrame(cursor.fetchall())

        cursor.execute("SELECT * FROM sales_raw")
        sales_df = pd.DataFrame(cursor.fetchall())

        empty_region_sales = sales_df[sales_df["region"] == ""]

        # -------------------------------------------------
        # CASE 1: Cannot infer -> fill Unknown
        # -------------------------------------------------
        if customers_df.empty or orders_df.empty:
            for _, row in empty_region_sales.iterrows():
                cursor.execute(
                    "UPDATE sales_raw SET region=%s WHERE sale_id=%s",
                    ("Unknown", row["sale_id"])
                )
            conn.commit()

        else:
            # -------------------------------------------------
            # CASE 2: Infering region using customers.city
            # -------------------------------------------------
            for _, row in empty_region_sales.iterrows():
                sale_id = row["sale_id"]

                related_orders = orders_df[orders_df["order_id"] == row["order_id"]]

                if related_orders.empty:
                    final_region = "Unknown"
                else:
                    customer_ids = related_orders["customer_id"].tolist()

                    related_customers = customers_df[
                        (customers_df["customer_id"].isin(customer_ids)) &
                        (customers_df["city"] != "")
                    ]

                    final_region = related_customers.iloc[0]["city"] if not related_customers.empty else "Unknown"

                cursor.execute(
                    "UPDATE sales_raw SET region=%s WHERE sale_id=%s",
                    (final_region, sale_id)
                )

            conn.commit()

        # -------------------------------------------------
        # STEP 4: Clean region
        # -------------------------------------------------
        cursor.execute("""
            UPDATE sales_raw
            SET region = TRIM(region)
            WHERE region IS NOT NULL;
        """)

        cursor.execute("""
            UPDATE sales_raw
            SET region = CONCAT(
                UPPER(LEFT(region, 1)),
                LOWER(SUBSTRING(region, 2))
            )
            WHERE region IS NOT NULL AND region != '';
        """)

        conn.commit()
        return True

    except Error as e:
        print(f"Error during preprocessing sales: {e}")
        return False

    finally:
        cursor.close()
        conn.close()



# =====================================================
# PREPROCESS PRODUCTS
# =====================================================
def preprocess_raw_product():
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        # Empty name -> Unnamed Product
        cursor.execute("""
            UPDATE products_raw
            SET product_name='Unnamed Product'
            WHERE TRIM(product_name) = '' OR product_name IS NULL;
        """)

        # Empty category -> Unknown
        cursor.execute("""
            UPDATE products_raw
            SET category='Unknown'
            WHERE TRIM(category) = '' OR category IS NULL;
        """)

        conn.commit()

        # Trim spaces
        cursor.execute("""
            UPDATE products_raw
            SET product_name = TRIM(product_name),
                category = TRIM(category),
                selling_price = TRIM(selling_price),
                cost_price = TRIM(cost_price);
        """)
        conn.commit()

        # Remove multiple spaces
        cursor.execute("""
            UPDATE products_raw
            SET product_name = REGEXP_REPLACE(product_name, '\\s+', ' ');
        """)
        conn.commit()

        # Title-case name + category
        cursor.execute("SELECT product_id, product_name, category FROM products_raw")
        rows = cursor.fetchall()

        for r in rows:
            clean_name = r["product_name"].title()
            clean_cat = r["category"].title()

            cursor.execute("""
                UPDATE products_raw
                SET product_name=%s, category=%s
                WHERE product_id=%s
            """, (clean_name, clean_cat, r["product_id"]))

        conn.commit()

        # Converting price strings -> float
        cursor.execute("SELECT product_id, selling_price, cost_price FROM products_raw")
        rows = cursor.fetchall()

        def clean_price(v):
            if v is None:
                return 0
            v = str(v).replace(",", "").replace("₹", "").replace("$", "").strip()
            try:
                return float(v)
            except:
                return 0

        for r in rows:
            pid = r["product_id"]
            sp = clean_price(r["selling_price"])
            cp = clean_price(r["cost_price"])

            cursor.execute("""
                UPDATE products_raw
                SET selling_price=%s, cost_price=%s
                WHERE product_id=%s
            """, (sp, cp, pid))

        conn.commit()

        # Stock cannot be negative
        cursor.execute("""
            UPDATE products_raw
            SET stock = CASE WHEN stock < 0 THEN 0 ELSE stock END;
        """)
        conn.commit()

        return True

    except Error as e:
        print(f"Error during preprocessing products: {e}")
        return False

    finally:
        cursor.close()
        conn.close()



# =====================================================
# MASTER FUNCTION
# =====================================================
def preprocess_all_raw():
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)

    results = {"customers": "", "sales": "", "products": ""}

    try:
        cursor.execute("SELECT COUNT(*) AS c FROM customers_raw")
        cust_count = cursor.fetchone()["c"]

        cursor.execute("SELECT COUNT(*) AS c FROM sales_raw")
        sales_count = cursor.fetchone()["c"]

        cursor.execute("SELECT COUNT(*) AS c FROM products_raw")
        prod_count = cursor.fetchone()["c"]

        # Process customers
        if cust_count > 0:
            results["customers"] = "Success" if preprocess_raw_customer() else "Failed"
        else:
            results["customers"] = "Skipped (customers_raw empty)"

        # Process sales
        if sales_count > 0:
            results["sales"] = "Success" if preprocess_raw_sale() else "Failed"
        else:
            results["sales"] = "Skipped (sales_raw empty)"

        # Process products
        if prod_count > 0:
            results["products"] = "Success" if preprocess_raw_product() else "Failed"
        else:
            results["products"] = "Skipped (products_raw empty)"

        return results

    except Exception as e:
        return {"error": str(e)}

    finally:
        cursor.close()
        conn.close()
