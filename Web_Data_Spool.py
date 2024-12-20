import streamlit as st
import pandas as pd
import psycopg2
from io import BytesIO

# Function to connect to PostgreSQL and fetch data
def fetch_data(start_date, end_date, table_name, db_params):
    try:
        # PostgreSQL connection
        conn = psycopg2.connect(
            host=db_params["host"],
            port=db_params["port"],
            database=db_params["database"],
            user=db_params["user"],
            password=db_params["password"]
        )
        # Use the provided table name in the SQL query
        query = f"""
            SELECT * 
            FROM {table_name}
            WHERE transaction_date BETWEEN %s AND %s
        """
        # Fetch data into DataFrame
        df = pd.read_sql_query(query, conn, params=(start_date, end_date))
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return None

# Function to export DataFrame to Excel
def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
    processed_data = output.getvalue()
    return processed_data

# Streamlit App
def main():
    st.title("PostgreSQL Data Export Tool")
    st.write("Fetch data from PostgreSQL by specifying a table, date range, and export it as CSV or Excel.")

    # Sidebar for database connection parameters
    '''
    st.sidebar.header("Database Connection")
    db_params = {
        "host": st.sidebar.text_input("Host", value="localhost"),
        "port": st.sidebar.text_input("Port", value="5432"),
        "database": st.sidebar.text_input("Database", value="your_database"),
        "user": st.sidebar.text_input("User", value="your_username"),
        "password": st.sidebar.text_input("Password", type="password")
    }           
   '''
    # Fetch secrets from Streamlit's secrets manager
    db_params = {
       "host": st.secrets["bitnob-servers.postgres.database.azure.com"], 
       "port": st.secrets["5432"], 
       "user": st.secrets["data_bitnob_team"], 
       "database": st.secrets["bitnob_db"],
       "password": st.secrets["050A0A7N2T_3@NT"]
    }
    
    # Mapping of user-friendly table names to actual table names
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
        data = fetch_data(start_date_str, end_date_str, table_name, db_params)
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
