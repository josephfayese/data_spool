import streamlit as st
import pandas as pd
from urllib.parse import quote_plus
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine
from io import BytesIO

# Function to fetch data in chunks
def fetch_data_in_chunks(start_date, end_date, table_name, db_params, chunk_size=50000):
    try:
        # Encode the password to handle special characters
        password = quote_plus(db_params["password"])
        
        # Using SQLAlchemy for efficient DB connection and query handling
        engine = create_engine(
            f"postgresql+psycopg2://{db_params['user']}:{password}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
        )
        conn = engine.connect()
        
        # Define query with placeholders
        query = f"""
            SELECT * 
            FROM {table_name}
            WHERE transaction_date BETWEEN :start_date AND :end_date
        """
        
        # Initialize an empty DataFrame
        data = pd.DataFrame()
        
        # Fetch data in chunks
        for chunk in pd.read_sql_query(
            query,
            conn,
            params={"start_date": start_date, "end_date": end_date},
            chunksize=chunk_size,
        ):
            data = pd.concat([data, chunk], ignore_index=True)
        
        conn.close()
        return data
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return None

# Function to export DataFrame to Excel
def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
    return output.getvalue()

# Streamlit App
def main():
    st.title("PostgreSQL Data Export Tool")
    st.write("Fetch data from PostgreSQL by specifying a table, date range, and export it as CSV or Excel.")

    # Fetch secrets from Streamlit's secrets manager
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

    # Table mapping
    table_mapping = {
        "Deposit": "data_spool.b2c_collections",
        "Withdrawals": "data_spool.b2c_payouts",
        "Trongrid": "data_spool.trongrid",
        "OKX Data": "data_spool.okx_data",
        "App Transactions": "data_spool.in_app_transactions",
        "Nobblet for Finance": "data_spool.nobblet_finance",
        "Bitnob for Nobblet": "data_spool.nobblet_bitnob_records",
    }

    # Dropdown for table selection
    st.subheader("Select Table to Fetch Data From")
    selected_table = st.selectbox("Table Name", list(table_mapping.keys()))
    table_name = table_mapping[selected_table]

    # Date inputs for filtering data
    st.subheader("Specify Date Range")
    start_date = st.date_input("Start Date")
    end_date = st.date_input("End Date")
    
    # Convert dates to 'YYYY-MM-DD' string format
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    if start_date > end_date:
        st.error("Start date cannot be after end date!")
        return

    # Button to fetch data
    if st.button("Fetch Data"):
        st.info(f"Fetching data from table: {selected_table}...")
        data = fetch_data_in_chunks(start_date_str, end_date_str, table_name, db_params)
        if data is not None and not data.empty:
            st.success("Data fetched successfully!")
            st.write(f"Number of records: {len(data)}")

            # Display data
            st.dataframe(data)

            # Export options
            st.subheader("Export Data")
            col1, col2 = st.columns(2)

            with col1:
                # Export as CSV
                csv = data.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download CSV",
                    data=csv,
                    file_name=f"{selected_table.replace(' ', '_').lower()}_data_export.csv",
                    mime="text/csv"
                )

            with col2:
                # Export as Excel
                excel = to_excel(data)
                st.download_button(
                    label="📥 Download Excel",
                    data=excel,
                    file_name=f"{selected_table.replace(' ', '_').lower()}_data_export.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        else:
            st.warning("No data found for the specified date range or table!")

if __name__ == "__main__":
    main()
