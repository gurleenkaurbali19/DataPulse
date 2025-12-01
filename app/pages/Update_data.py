# app/pages/update_data.py
import streamlit as st
import pandas as pd
import os, sys
import numpy as np

# Add project root (two levels up) so utils can be imported
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from utils.database_connection import create_connection
from utils.update_data_operations import (
    update_customer_raw, update_customer_main,
    update_product_raw, update_product_main,
    update_order_raw, update_order_main,
    update_sale_raw, update_sale_main,
)

st.set_page_config(page_title="Update Data", layout="wide")
st.title("🔁 Update Data Portal")
st.write("Choose Raw/Main → Table → Record. Edit allowed fields and click Update.")

# -----------------------
# Helpers
# -----------------------
def fetchall_df(query, params=()):
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    finally:
        cursor.close()
        conn.close()

def fetch_one(query, params=()):
    conn = create_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(query, params)
        return cursor.fetchone()
    finally:
        cursor.close()
        conn.close()

def to_native(v):
    """Convert pandas/numpy types to plain Python types for display & DB calls."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    return v

# ----------
# Table metadata
# ----------
TABLE_META = {
    "customers_raw": {"pk": "customer_id", "label_cols": ["customer_name", "phone"], "list_q": "SELECT customer_id, customer_name, phone FROM customers_raw ORDER BY customer_id", "select_q": "SELECT * FROM customers_raw WHERE customer_id=%s"},
    "customers": {"pk": "customer_id", "label_cols": ["customer_name", "phone"], "list_q": "SELECT customer_id, customer_name, phone FROM customers ORDER BY customer_id", "select_q": "SELECT * FROM customers WHERE customer_id=%s"},
    "products_raw": {"pk": "product_id", "label_cols": ["product_name", "category"], "list_q": "SELECT product_id, product_name, category FROM products_raw ORDER BY product_id", "select_q": "SELECT * FROM products_raw WHERE product_id=%s"},
    "products": {"pk": "product_id", "label_cols": ["product_name", "category"], "list_q": "SELECT product_id, product_name, category FROM products ORDER BY product_id", "select_q": "SELECT * FROM products WHERE product_id=%s"},
    "orders_raw": {"pk": "order_id", "label_cols": ["order_id", "customer_id"], "list_q": "SELECT order_id, customer_id, product_id FROM orders_raw ORDER BY order_id", "select_q": "SELECT * FROM orders_raw WHERE order_id=%s"},
    "orders": {"pk": "order_id", "label_cols": ["order_id", "customer_id"], "list_q": "SELECT order_id, customer_id, product_id FROM orders ORDER BY order_id", "select_q": "SELECT * FROM orders WHERE order_id=%s"},
    "sales_raw": {"pk": "sale_id", "label_cols": ["sale_id", "order_id"], "list_q": "SELECT sale_id, order_id FROM sales_raw ORDER BY sale_id", "select_q": "SELECT * FROM sales_raw WHERE sale_id=%s"},
    "sales": {"pk": "sale_id", "label_cols": ["sale_id", "order_id"], "list_q": "SELECT sale_id, order_id FROM sales ORDER BY sale_id", "select_q": "SELECT * FROM sales WHERE sale_id=%s"},
}

# ----------
# Top selectors
# ----------
col1, col2 = st.columns([1, 2])
with col1:
    table_type = st.selectbox("Choose table set", ["Raw", "Main"], key="tbl_type")
with col2:
    tables = (["customers_raw", "products_raw", "orders_raw", "sales_raw"] if table_type == "Raw" else ["customers", "products", "orders", "sales"])
    table_choice = st.selectbox("Choose table", tables, key="tbl_choice")

meta = TABLE_META[table_choice]

# fetch listing rows for dropdown
list_df = fetchall_df(meta["list_q"])
if list_df.empty:
    st.info("No records available for this table.")
    st.stop()

# Build quick maps to make labels (no per-row DB calls)
def build_customer_map(raw_or_main):
    tbl = "customers_raw" if raw_or_main == "Raw" else "customers"
    df = fetchall_df(f"SELECT customer_id, customer_name FROM {tbl}")
    if df.empty:
        return {}
    df["customer_id"] = df["customer_id"].astype(int)
    return df.set_index("customer_id")["customer_name"].to_dict()

customer_map_raw = build_customer_map("Raw")
customer_map_main = build_customer_map("Main")

def build_order_to_customer_map(raw_or_main):
    tbl = "orders_raw" if raw_or_main == "Raw" else "orders"
    df = fetchall_df(f"SELECT order_id, customer_id FROM {tbl}")
    if df.empty:
        return {}
    return df.set_index("order_id")["customer_id"].to_dict()

order_to_cust_raw = build_order_to_customer_map("Raw")
order_to_cust_main = build_order_to_customer_map("Main")

# Build options list (label, pk)
options = []
if table_choice in ("orders_raw", "orders"):
    cust_map = customer_map_raw if table_type == "Raw" else customer_map_main
    for _, r in list_df.iterrows():
        oid = to_native(r.get("order_id"))
        cid = to_native(r.get("customer_id"))
        label = f"{oid} | {cust_map.get(cid, 'Unknown')}"
        options.append((label, oid))
elif table_choice in ("sales_raw", "sales"):
    cust_map = customer_map_raw if table_type == "Raw" else customer_map_main
    order_to_cust = order_to_cust_raw if table_type == "Raw" else order_to_cust_main
    for _, r in list_df.iterrows():
        pk_val = to_native(r.get(meta["pk"]))
        order_id = to_native(r.get("order_id"))
        cust_id = order_to_cust.get(order_id)
        label = f"{pk_val} | order:{order_id} | {cust_map.get(cust_id, 'Unknown')}"
        options.append((label, pk_val))
else:
    for _, r in list_df.iterrows():
        parts = []
        for col in meta["label_cols"]:
            parts.append(str(r.get(col, "")))
        label = " | ".join([p for p in parts if p != ""]) or str(to_native(r.get(meta["pk"])))
        options.append((label, to_native(r.get(meta["pk"]))))

# sort options
def _sort_key(item):
    first = item[0].split(" | ")[0]
    try:
        return (0, int(first))
    except Exception:
        return (1, item[0])
options = sorted(options, key=_sort_key)
labels = [o[0] for o in options]
pk_map = {o[0]: o[1] for o in options}

selected_label = st.selectbox("Select record", labels, key="record_select")
selected_pk = pk_map[selected_label]
selected_pk = int(selected_pk)  # make sure native int

# fetch selected record
record_row = fetch_one(meta["select_q"], (selected_pk,))
if not record_row:
    st.error("Could not load the selected record.")
    st.stop()

st.markdown("---")
st.subheader(f"Editing record in `{table_choice}`")

# ---------- Editable rules (confirmed) ----------
EDIT_RULES = {
    "customers_raw": {"readonly": ["customer_id", "created_at"], "editable": ["customer_name", "email", "phone", "city"]},
    "customers": {"readonly": ["customer_id", "created_at", "phone"], "editable": ["customer_name", "email", "city"]},
    "products_raw": {"readonly": ["product_id", "added_at"], "editable": ["product_name", "category", "selling_price", "cost_price", "stock"]},
    "products": {"readonly": ["product_id", "product_name", "category", "added_at"], "editable": ["selling_price", "cost_price", "stock"]},
    "orders_raw": {"readonly": ["order_id", "customer_id", "product_id", "order_date"], "editable": ["quantity", "order_status", "payment_method"]},
    "orders": {"readonly": ["order_id", "customer_id", "product_id", "quantity", "order_date"], "editable": ["order_status", "payment_method"]},
    "sales_raw": {"readonly": ["sale_id", "order_id", "sale_amount", "profit", "sale_date"], "editable": ["region"]},
    "sales": {"readonly": ["sale_id", "order_id", "sale_amount", "profit", "sale_date"], "editable": ["region"]},
}
rules = EDIT_RULES[table_choice]

ORDER_STATUS_OPTIONS = ["Pending", "Processing", "Shipped", "Delivered", "Cancelled", "Returned"]
PAYMENT_METHOD_OPTIONS = ["Credit Card", "Debit Card", "UPI", "Cash on Delivery", "Net Banking", "Wallet"]

# ---------- Form ----------
with st.form("update_form", clear_on_submit=False):
    left, right = st.columns(2)
    updated = {}
    cols = list(record_row.keys())

    for i, col in enumerate(cols):
        val = record_row.get(col)
        target = left if i % 2 == 0 else right
        widget_key = f"{table_choice}_{col}_{selected_pk}"
        with target:
            if col in rules["readonly"]:
                st.text_input(col, value="" if val is None else str(val), disabled=True, key=f"ro_{widget_key}")
                updated[col] = to_native(val)
            else:
                if col in ("selling_price", "cost_price", "sale_amount", "profit"):
                    try:
                        num = float(val) if val is not None else 0.0
                    except Exception:
                        num = 0.0
                    updated[col] = st.number_input(col, value=num, step=0.01, format="%.2f", key=f"num_{widget_key}")
                elif col in ("stock", "quantity"):
                    try:
                        num = int(val) if val is not None else 0
                    except Exception:
                        num = 0
                    updated[col] = st.number_input(col, value=num, step=1, format="%d", key=f"int_{widget_key}")
                elif col == "order_status":
                    default = val if val is not None else ORDER_STATUS_OPTIONS[0]
                    idx = ORDER_STATUS_OPTIONS.index(default) if default in ORDER_STATUS_OPTIONS else 0
                    updated[col] = st.selectbox(col, ORDER_STATUS_OPTIONS, index=idx, key=f"sel_{widget_key}")
                elif col == "payment_method":
                    default = val if val is not None else PAYMENT_METHOD_OPTIONS[0]
                    idx = PAYMENT_METHOD_OPTIONS.index(default) if default in PAYMENT_METHOD_OPTIONS else 0
                    updated[col] = st.selectbox(col, PAYMENT_METHOD_OPTIONS, index=idx, key=f"pay_{widget_key}")
                else:
                    updated[col] = st.text_input(col, value="" if val is None else str(val), key=f"text_{widget_key}")

    st.markdown("---")
    submit = st.form_submit_button("Update record")

# Persist success message across reruns using st.session_state
if "update_success_message" in st.session_state and st.session_state.update_success_message:
    st.success(st.session_state.update_success_message)
    # clear it so it doesn't show repeatedly
    st.session_state.update_success_message = None

# ---------- Validation for MAIN tables ----------
def validate_main(table, values):
    if table == "customers":
        for f in ("customer_name", "email", "city"):
            if not str(values.get(f, "")).strip():
                return False, f"{f} cannot be empty for main customers."
    if table == "products":
        for f in ("selling_price", "cost_price"):
            try:
                v = values.get(f)
                if v is None or str(v) == "":
                    return False, f"{f} cannot be empty for main products."
                if float(v) < 0:
                    return False, f"{f} cannot be negative."
            except Exception:
                return False, f"{f} must be numeric."
    if table == "orders":
        for f in ("order_status", "payment_method"):
            if not str(values.get(f, "")).strip():
                return False, f"{f} cannot be empty for main orders."
    if table == "sales":
        if not str(values.get("region", "")).strip():
            return False, "region cannot be empty for main sales."
    return True, None

# ---------- On submit ----------
if submit:
    ok, err = validate_main(table_choice, updated)
    if not ok:
        st.error(err)
    else:
        try:
            resp = None

            if table_choice == "customers_raw":
                resp = update_customer_raw(
                    int(selected_pk),
                    str(updated.get("customer_name") or "").strip(),
                    (str(updated.get("email") or "").strip() or None),
                    str(updated.get("phone") or "").strip(),
                    (str(updated.get("city") or "").strip() or None),
                )

            elif table_choice == "customers":
                resp = update_customer_main(
                    int(selected_pk),
                    str(updated.get("customer_name") or "").strip(),
                    str(updated.get("email") or "").strip(),
                    str(updated.get("city") or "").strip(),
                )

            elif table_choice == "products_raw":
                resp = update_product_raw(
                    int(selected_pk),
                    str(updated.get("product_name") or "").strip(),
                    str(updated.get("category") or "").strip(),
                    None if updated.get("selling_price") in (None, "") else float(updated.get("selling_price")),
                    None if updated.get("cost_price") in (None, "") else float(updated.get("cost_price")),
                    None if updated.get("stock") in (None, "") else int(updated.get("stock")),
                )

            elif table_choice == "products":
                resp = update_product_main(
                    int(selected_pk),
                    float(updated.get("selling_price") or 0.0),
                    float(updated.get("cost_price") or 0.0),
                    int(updated.get("stock") or 0),
                )

            elif table_choice == "orders_raw":
                resp = update_order_raw(
                    int(selected_pk),
                    int(updated.get("quantity") or 0),
                    str(updated.get("order_status") or ""),
                    str(updated.get("payment_method") or ""),
                )

            elif table_choice == "orders":
                resp = update_order_main(
                    int(selected_pk),
                    str(updated.get("order_status") or ""),
                    str(updated.get("payment_method") or ""),
                )

            elif table_choice == "sales_raw":
                resp = update_sale_raw(
                    int(selected_pk),
                    str(updated.get("region") or ""),
                )

            elif table_choice == "sales":
                resp = update_sale_main(
                    int(selected_pk),
                    str(updated.get("region") or ""),
                )

            else:
                st.error("Unsupported table selection.")
                st.stop()

            # show result and refresh
            if resp and resp.get("status") == "success":
                # persist success message across rerun
                st.session_state.update_success_message = resp.get("message")
                # use modern rerun
                try:
                    st.rerun()
                except Exception:
                    from streamlit import rerun as _rerun
                    _rerun()
            else:
                st.error(resp.get("message") if resp else "Update failed")

        except Exception as e:
            st.exception(e)
