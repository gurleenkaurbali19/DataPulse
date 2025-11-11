from utils.database_connection import create_connection
from mysql.connector import Error

#INSERT FUNCTIONS FOR RAW TABLES:

def insert_raw_customer(customer_name, email, phone ,city):
    conn = create_connection()
    if not conn:
        print("Database connection unsuccessful!")
        return False
    cursor=conn.cursor()
    try:
        query='''INSERT INTO customers_raw (customer_name , email , phone , city) 
        values (%s, %s, %s, %s);
        '''
        values=(customer_name, email, phone, city)
        cursor.execute(query,values)
        conn.commit()

        print(f"Successfully inserted customer: {customer_name}")
        return True
    except Error as e:
        print(f"Error inserting data into customers_raw: {e}")
        return False
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("Connection closed.")

def insert_raw_product(product_name, category, selling_price, cost_price,stock):
    conn=create_connection()
    if not conn:
        print("Database connection unsuccessful!!")
        return False
    cursor=conn.cursor()
    try:
        query='''INSERT INTO products_raw (product_name, category,selling_price, cost_price,stock)
        values (%s,%s,%s,%s,%s);
        '''
        values=(product_name, category, selling_price, cost_price,stock)
        cursor.execute(query,values)
        conn.commit()
        print(f"Successfully inserted product: {product_name}")
        return True
    except Error as e:
        print(f"Error inserting data into products_raw: {e}")
        return False
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("Connection closed.")


def insert_raw_order(customer_id, product_id, quantity, order_status, payment_method):
    conn=create_connection()
    if not conn:
        print("Database connection unsuccessful!!")
        return False
    cursor=conn.cursor()
    try:
        query='''INSERT INTO orders_raw (customer_id, product_id, quantity, order_status, payment_method)
        values (%s, %s, %s, %s, %s);
    '''
        values=(customer_id, product_id, quantity, order_status, payment_method)
        cursor.execute(query,values)
        conn.commit()
        print(f"Successfully inserted order")
        return True
    except Error as e:
        print(f"Error inserting data into orders_raw: {e}")
        return False
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("Connection closed.")
    
def insert_raw_sale(order_id, sale_amount, profit, region):
    conn=create_connection()
    if not conn:
        print("Database connection unsuccessful!!")
        return False
    cursor=conn.cursor()
    try:
        query='''INSERT INTO sales_raw (order_id, sale_amount, profit, region)
        values (%s, %s, %s, %s);
    '''
        values=(order_id, sale_amount, profit, region)
        cursor.execute(query,values)
        conn.commit()
        print(f"Successfully inserted sale")
        return True
    except Error as e:
        print(f"Error inserting data into sales_raw: {e}")
        return False
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("Connection closed.")
    

#INSERT FUNCTIONS FOR MAIN TABLES:

def insert_customer(customer_id,customer_name, email, phone ,city,created_at):
    conn = create_connection()
    if not conn:
        print("Database connection unsuccessful!")
        return False
    cursor=conn.cursor()
    try:
        query='''INSERT INTO customers (customer_id,customer_name, email, phone ,city,created_at) 
        values (%s, %s, %s, %s, %s, %s);
        '''
        values=(customer_id,customer_name, email, phone ,city,created_at)
        cursor.execute(query,values)
        conn.commit()

        print(f"Successfully inserted customer into the database: {customer_name}")
        return True
    except Error as e:
        print(f"Error inserting data into customers: {e}")
        return False
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("Connection closed.")

def insert_product(product_id,product_name, category, selling_price, cost_price,stock,added_at):
    conn=create_connection()
    if not conn:
        print("Database connection unsuccessful!!")
        return False
    cursor=conn.cursor()
    try:
        query='''INSERT INTO products (product_id,product_name, category, selling_price, cost_price,stock,added_at)
        values (%s,%s,%s,%s,%s,%s,%s);
        '''
        values=(product_id,product_name, category, selling_price, cost_price,stock,added_at)
        cursor.execute(query,values)
        conn.commit()
        print(f"Successfully inserted product into database: {product_name}")
        return True
    except Error as e:
        print(f"Error inserting data into products: {e}")
        return False
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("Connection closed.")

def insert_order(order_id,customer_id, product_id, quantity, order_status, payment_method,order_date):
    conn=create_connection()
    if not conn:
        print("Database connection unsuccessful!!")
        return False
    cursor=conn.cursor()
    try:
        query='''INSERT INTO orders (order_id,customer_id, product_id, quantity, order_status, payment_method,order_date)
        values (%s, %s, %s, %s, %s, %s, %s);
    '''
        values=(order_id,customer_id, product_id, quantity, order_status, payment_method,order_date)
        cursor.execute(query,values)
        conn.commit()
        print(f"Successfully inserted order into database!!")
        return True
    except Error as e:
        print(f"Error inserting data into orders: {e}")
        return False
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("Connection closed.")

def insert_sale(sale_id,order_id, sale_amount, profit, region,sale_date):
    conn=create_connection()
    if not conn:
        print("Database connection unsuccessful!!")
        return False
    cursor=conn.cursor()
    try:
        query='''INSERT INTO sales (sale_id,order_id, sale_amount, profit, region,sale_date)
        values (%s, %s, %s, %s,%s,%s);
    '''
        values=(sale_id,order_id, sale_amount, profit, region,sale_date)
        cursor.execute(query,values)
        conn.commit()
        print(f"Successfully inserted sale into database!!")
        return True
    except Error as e:
        print(f"Error inserting data into sales: {e}")
        return False
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("Connection closed.")
    