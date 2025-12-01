import streamlit as st
import sys, os
import pandas as pd
import mysql.connector
import sys, os
import streamlit as st

# Add project root to sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append(PROJECT_ROOT)
from utils.database_insert_operations import (
    insert_raw_customer,
    insert_raw_product,
    insert_raw_order,
    insert_raw_sale
    
)
from utils.database_retrieve_operations import (
    retrieve_table,
    retrieve_all_customers,
    retrieve_all_products,
    retrieve_all_orders,
)


st.title("📊 Data Entry Portal")

st.write("Add or view entries in your raw database tables below.")


# ---------- CUSTOMER FORM ----------
st.subheader("👤 Add Customer")
with st.form("add_customer_form"):
    customer_name = st.text_input("Customer Name *")
    email = st.text_input("Email")
    phone = st.text_input("Phone *")
    city = st.text_input("City")
    submit_customer = st.form_submit_button("Insert Customer")

    if submit_customer:
        if not customer_name.strip():
            st.error("Customer Name is required!")
        elif not phone.strip():
            st.error("Customer Phone is required!")
        else:
            try:
                success = insert_raw_customer(customer_name, email, phone, city)
                if success:
                    st.success(f"✅ Customer '{customer_name}' inserted successfully!")
                else:
                    st.error("❌ Failed to insert customer.")
            except mysql.connector.errors.IntegrityError as e:
                if "Duplicate" in str(e):
                    st.error("Customer with this phone or email already exists!")
                else:
                    st.error(f"Database error: {e}")
            except Exception as e:
                st.error(f"Unexpected error: {e}")


# ---------- PRODUCT FORM ----------
st.subheader("📦 Add Product")
with st.form("add_product_form"):
    product_name = st.text_input("Product Name *")
    category = st.text_input("Category *")
    selling_price = st.number_input("Selling Price *", min_value=0.0, step=0.01)
    cost_price = st.number_input("Cost Price *", min_value=0.0, step=0.01)
    stock = st.number_input("Stock", min_value=0, step=1)

    submit_product = st.form_submit_button("Insert Product")

    if submit_product:
        if not product_name.strip():
            st.error("Product Name is required!")
        elif not category.strip():
            st.error("Product Category is required!")
        elif selling_price <= 0:
            st.error("Selling Price must be greater than zero!")
        elif cost_price < 0:
            st.error("Cost Price cannot be negative!")
        else:
            try:
                success = insert_raw_product(product_name, category, selling_price, cost_price, stock)
                if success:
                    st.success(f"✅ Product '{product_name}' inserted successfully!")
                else:
                    st.error("❌ Failed to insert product.")
            except mysql.connector.errors.IntegrityError as e:
                if "Duplicate" in str(e):
                    st.error("Product with this name already exists!")
                else:
                    st.error(f"Database error: {e}")
            except Exception as e:
                st.error(f"Unexpected error: {e}")


# ---------- ORDER FORM ----------
st.subheader("🧾 Add Order")

customers = retrieve_all_customers() or []
products = retrieve_all_products() or []

if not customers or not products:
    st.warning("⚠️ Please add at least one customer and one product first.")
else:
    customer_options = {
        f"{c['customer_name']} ({c['source']})": c['customer_id']
        for c in customers
    }

    product_options = {
        f"{p['product_name']} ({p['source']})": p['product_id']
        for p in products
    }

    with st.form("add_order_form"):
        customer_id = st.selectbox(
            "Select Customer",
            options=list(customer_options.values()),
            format_func=lambda x: next(k for k, v in customer_options.items() if v == x)
        )
        product_id = st.selectbox(
            "Select Product",
            options=list(product_options.values()),
            format_func=lambda x: next(k for k, v in product_options.items() if v == x)
        )
        quantity = st.number_input("Quantity", min_value=1, step=1)
        order_status = st.selectbox(
            "Order Status",
            ["Pending", "Processing", "Shipped", "Delivered", "Cancelled", "Returned"]
        )
        payment_method = st.selectbox(
            "Payment Method",
            ["Credit Card", "Debit Card", "UPI", "Cash on Delivery", "Net Banking", "Wallet"]
        )

        submit_order = st.form_submit_button("Insert Order")

        if submit_order:
            if insert_raw_order(customer_id, product_id, quantity, order_status, payment_method):
                st.success("✅ Order inserted successfully!")
            else:
                st.error("❌ Failed to insert order.")


# ---------- SALE FORM (Auto-calculated) ----------
st.subheader("💰 Add Sale")

orders = retrieve_all_orders()
customers = retrieve_all_customers()
products = retrieve_all_products()

if not orders:
    st.warning("⚠️ Please add at least one order first.")
    st.stop()

cust_map = {c["customer_id"]: c["customer_name"] for c in customers}

order_options = {
    f"{cust_map[o['customer_id']]} - Order #{o['order_id']} ({o['source']})": o['order_id']
    for o in orders
}

order_label = st.selectbox("Select Customer and Order", options=list(order_options.keys()))
order_id = order_options[order_label]

selected_order = next(o for o in orders if o["order_id"] == order_id)

selected_product = next(
    p for p in products if p["product_id"] == selected_order["product_id"]
)

quantity = selected_order["quantity"]
sale_amount = selected_product["selling_price"] * quantity
profit = (selected_product["selling_price"] - selected_product["cost_price"]) * quantity

with st.form("add_sale_form"):
    region = st.text_input("Region")
    st.number_input("Sale Amount (Auto)", value=float(sale_amount), step=0.01, disabled=True)
    st.number_input("Profit (Auto)", value=float(profit), step=0.01, disabled=True)

    submit_sale = st.form_submit_button("Insert Sale")

    if submit_sale:
        if insert_raw_sale(order_id, sale_amount, profit, region):
            st.success(f"✅ Sale for {order_label} inserted successfully!")
        else:
            st.error("❌ Failed to insert sale.")
