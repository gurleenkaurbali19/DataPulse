import sys, os
import streamlit as st

# Add project root to sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append(PROJECT_ROOT)
from utils.database_retrieve_operations import (
    retrieve_table,
)

st.set_page_config(page_title="View Data", layout="wide")
st.title("📊 View Data")

st.subheader("Select Table to View")

main_tables = ["customers", "products", "orders", "sales"]
raw_tables = ["customers_raw", "products_raw", "orders_raw", "sales_raw"]

category = st.selectbox("Choose Category", ["Main Tables", "Raw Tables"])

if category == "Main Tables":
    selected_table = st.selectbox("Select Table", main_tables)
else:
    selected_table = st.selectbox("Select Table", raw_tables)

if st.button("View Table"):
    data = retrieve_table(selected_table)
    if data:
        st.dataframe(data, use_container_width=True)
    else:
        st.error("Could not fetch data.")
