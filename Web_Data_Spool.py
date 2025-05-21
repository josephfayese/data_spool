import streamlit as st
import pandas as pd
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from io import BytesIO
import psutil
import gzip

# Fetch data with caching
@st.cache_data(show_spinner="Fetching data from database...")
def fetch_data_in_chunks(start_date, end_date, table_name, db_params, chunk_size=50000):
    try:
        password = quote_plus(db_params["password"])
        engine = create_engine(
            f"postgresql+psycopg2://{db_params['user']}:{password}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
        )
        conn = engine.connect()
        query = f"""
            SELECT * 
            FROM {table_name}
            WHERE transaction_date::date BETWEEN %s AND %s
        """

        chunks = []
        for chunk in pd.read_sql_query(query, con=conn, params=(start_date, end_date), chunksize=chunk_size):
            chunks.append(chunk)

        conn.close()
        return pd.concat(chunks, ignore_index=True)
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()

def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
    return output.getvalue()

def to_compressed_csv(df):
    out = BytesIO()
    with gzip.GzipFile(fileobj=out, mode='w') as f:
        f.write(df.to_csv(index=False).encode('utf-8'))
    return out.getvalue()

def display_memory_usage():
    mem = psutil.virtual_memory()
    st.caption(f"🔋 Memory used: {mem.percent:.2f}% — {mem.used // (1024**2)} MB / {mem.total // (1024**2)} MB")

# Streamlit UI
def main():
    st.set_page_config("PostgreSQL Data Export", layout="wide")
    st.title("📊 PostgreSQL Data Export Tool")
    st.markdown("Export filtered records from a PostgreSQL table as Excel or compressed CSV.")

    display_memory_usage()

    try:
        db_params = {
            "host": st.secrets["bitnob-servers"]["postgres"]["host"],
            "port": st.secrets["bitnob-servers"]["postgres"]["port"],
            "database": st.secrets["bitnob-servers"]["postgres"]["database"],
            "user": st.secrets["bitnob-servers"]["postgres"]["user"],
            "password": st.secrets["bitnob-servers"]["postgres"]["password"],
        }
    except KeyError as e:
        st.error(f"Missing key in secrets: {e}")
        st.stop()

    table_mapping = {
        "Deposit": "data_spool.b2c_collections",
        "Withdrawals": "data_spool.b2c_payouts",
        "Trongrid": "data_spool.trongrid",
        "OKX Data": "data_spool.okx_data",
        "App Transactions": "data_spool.in_app_transactions",
        "Nobblet for Finance": "data_spool.nobblet_finance",
        "Bitnob for Nobblet": "data_spool.nobblet_bitnob_records"
    }

    table_choice = st.selectbox("Select Table", list(table_mapping.keys()))
    start_date = st.date_input("Start Date")
    end_date = st.date_input("End Date")

    if st.button("Fetch and Preview Data"):
        df = fetch_data_in_chunks(start_date, end_date, table_mapping[table_choice], db_params)
        if not df.empty:
            st.success(f"Fetched {len(df):,} rows.")
            rows_per_page = st.slider("Rows per page", 10, 100, 25)
            total_pages = (len(df) - 1) // rows_per_page + 1
            page = st.number_input("Page", 1, total_pages, 1)
            st.dataframe(df.iloc[(page - 1) * rows_per_page : page * rows_per_page])
            
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    "⬇️ Download as Excel",
                    to_excel(df),
                    file_name=f"{table_choice}_{start_date}_to_{end_date}.xlsx"
                )
            with col2:
                st.download_button(
                    "⬇️ Download as Compressed CSV (.gz)",
                    to_compressed_csv(df),
                    file_name=f"{table_choice}_{start_date}_to_{end_date}.csv.gz"
                )
        else:
            st.warning("No data returned for the selected date range and table.")

if __name__ == "__main__":
    main()
