import streamlit as st
import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np
from datetime import datetime, date
from decimal import Decimal
import json
from typing import Dict, Any
import re

# Define the DataTypeHandler and MySQLDataUploader classes (your existing code)
class DataTypeHandler:
    def __init__(self):
        self.type_mapping = {
            'int8': 'TINYINT',
            'int16': 'SMALLINT',
            'int32': 'INT',
            'int64': 'BIGINT',
            'uint8': 'TINYINT UNSIGNED',
            'uint16': 'SMALLINT UNSIGNED',
            'uint32': 'INT UNSIGNED',
            'uint64': 'BIGINT UNSIGNED',
            'float32': 'FLOAT',
            'float64': 'DOUBLE',
            'decimal': 'DECIMAL(65,30)',
            'object': 'TEXT',
            'string': 'TEXT',
            'category': 'VARCHAR(255)',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'DATETIME',
            'datetime64[ns, UTC]': 'DATETIME',
            'timedelta64[ns]': 'TIME',
            'date': 'DATE'
        }
    
    def infer_mysql_type(self, series: pd.Series) -> str:
        """
        Infers the MySQL data type for a given Pandas Series.
        """
        dtype_str = str(series.dtype)
        if dtype_str in ['datetime64[ns]', 'datetime64[ns, UTC]']:
            return 'DATETIME'
        elif dtype_str == 'timedelta64[ns]':
            return 'TIME'
        elif dtype_str == 'date':
            return 'DATE'
        if dtype_str in self.type_mapping:
            return self.type_mapping[dtype_str]
        if dtype_str in ['object', 'string']:
            return self._infer_string_type(series)
        if pd.api.types.is_numeric_dtype(series):
            return self._infer_numeric_type(series)
        return 'TEXT'
    
    def _infer_string_type(self, series: pd.Series) -> str:
        non_null_values = series.dropna()
        if len(non_null_values) == 0:
            return 'TEXT'
        max_length = non_null_values.astype(str).str.len().max()
        if max_length <= 255:
            return f'VARCHAR({max_length})'
        elif max_length <= 65535:
            return 'TEXT'
        elif max_length <= 16777215:
            return 'MEDIUMTEXT'
        else:
            return 'LONGTEXT'
    
    def _infer_numeric_type(self, series: pd.Series) -> str:
        if series.isnull().all():
            return 'DOUBLE'
        non_null_values = series.dropna()
        if pd.api.types.is_integer_dtype(series) or all(float(x).is_integer() for x in non_null_values):
            min_val = non_null_values.min()
            max_val = non_null_values.max()
            if min_val >= 0:
                if max_val <= 255:
                    return 'TINYINT UNSIGNED'
                elif max_val <= 65535:
                    return 'SMALLINT UNSIGNED'
                elif max_val <= 4294967295:
                    return 'INT UNSIGNED'
                else:
                    return 'BIGINT UNSIGNED'
            else:
                if min_val >= -128 and max_val <= 127:
                    return 'TINYINT'
                elif min_val >= -32768 and max_val <= 32767:
                    return 'SMALLINT'
                elif min_val >= -2147483648 and max_val <= 2147483647:
                    return 'INT'
                else:
                    return 'BIGINT'
        return 'DOUBLE'

class MySQLDataUploader:
    def __init__(self, connection_config: Dict[str, Any]):
        self.config = connection_config
        self.connection = None
        self.cursor = None
        self.type_handler = DataTypeHandler()
    
    def connect(self) -> bool:
        try:
            self.connection = mysql.connector.connect(**self.config)
            self.cursor = self.connection.cursor(dictionary=True)
            return True
        except Error as e:
            st.error(f"⚠️ Connection Error: {str(e)}")
            return False
    
    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
    
    def create_table(self, table_name: str, df: pd.DataFrame) -> bool:
        try:
            clean_columns = [re.sub(r'[^\w]', '_', col.lower()) for col in df.columns]
            df.columns = clean_columns
            column_defs = []
            for col in df.columns:
                mysql_type = self.type_handler.infer_mysql_type(df[col])
                column_defs.append(f"{col} {mysql_type}")
            create_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {', '.join(column_defs)}
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            self.cursor.execute(create_query)
            self.connection.commit()
            return True
        except Error as e:
            st.error(f"⚠️ Table Creation Error: {str(e)}")
            return False
    
    def insert_data(self, table_name: str, df: pd.DataFrame) -> bool:
        try:
            columns = df.columns.tolist()
            placeholders = ', '.join(['%s'] * len(columns))
            insert_query = f"INSERT INTO {table_name} ({', '.join([f'{col}' for col in columns])}) VALUES ({placeholders})"
            batch_size = 1000
            total_rows = len(df)
            progress_bar = st.progress(0)
            for i in range(0, total_rows, batch_size):
                batch = df.iloc[i:i + batch_size]
                values = [tuple(self._prepare_value(val) for val in row) for _, row in batch.iterrows()]
                self.cursor.executemany(insert_query, values)
                self.connection.commit()
                progress = min(1.0, (i + batch_size) / total_rows)
                progress_bar.progress(progress)
            progress_bar.progress(1.0)
            return True
        except Error as e:
            st.error(f"⚠️ Data Insertion Error: {str(e)}")
            return False
    
    def _prepare_value(self, value: Any) -> Any:
        """
        Prepares the value for MySQL insertion, handling None, datetime, date, Decimal, etc.
        """
        if pd.isna(value):
            return None
        elif isinstance(value, (datetime, pd.Timestamp)):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(value, date):
            return value.strftime('%Y-%m-%d')
        elif isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, np.integer):
            return int(value)
        elif isinstance(value, np.floating):
            return float(value)
        elif isinstance(value, (dict, list)):
            return json.dumps(value)
        return value



def main():
    # Set page title and title icon
    st.set_page_config(
        page_title="MySQL Data Uploading Portal",
        page_icon='✨'
    )

    custom_css = """
        <style>
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        </style>
    """
    st.markdown(custom_css, unsafe_allow_html=True)

    st.header("🚀 MySQL Data Handler & Uploader", divider="rainbow")
    st.markdown("### Easily map your CSV or Excel data to a MySQL database 📊")

    # MySQL connection settings
    with st.form("connection_settings"):
        col1, col2 = st.columns(2)
        with col1:
            host = st.text_input("🔗 Host", "localhost")
            user = st.text_input("👤 Username", "root")
            database = st.text_input("📂 Database")
        with col2:
            port = st.text_input("🔢 Port", "3306")
            password = st.text_input("🔑 Password", type="password")
        submit = st.form_submit_button("🚪 Connect to MySQL")

    # MySQL connection logic
    if submit:
        config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database
        }
        uploader = MySQLDataUploader(config)
        if uploader.connect():
            st.success("✅ Connected successfully!")
            st.session_state['uploader'] = uploader

    # File upload and data handling
    if 'uploader' in st.session_state:
        st.subheader("📂 Upload Data")
        uploaded_file = st.file_uploader("Upload your CSV or Excel file here 📁", type=['csv', 'xlsx'])

        if uploaded_file:
            try:
                # Load data based on file type
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                elif uploaded_file.name.endswith('.xlsx'):
                    df = pd.read_excel(uploaded_file, engine='openpyxl')

                # Display data preview
                st.write("### Data Preview:")
                st.dataframe(df.head())

                # Display inferred MySQL data types
                st.markdown("### 🧪 Inferred MySQL Data Types")
                type_handler = DataTypeHandler()
                inferred_types = {col: type_handler.infer_mysql_type(df[col]) for col in df.columns}
                for col, dtype in inferred_types.items():
                    st.markdown(f"- **{col}**: `{dtype}`")

                # Table name input and upload
                table_name = st.text_input("📋 Enter Table Name")
                if st.button("📤 Upload to MySQL"):
                    if not table_name:
                        st.error("⚠️ Please enter a valid table name.")
                    else:
                        uploader = st.session_state['uploader']
                        if uploader.create_table(table_name, df):
                            if uploader.insert_data(table_name, df):
                                st.success(f"🎉 Successfully uploaded {len(df)} rows to `{table_name}`!")
            except Exception as e:
                st.error(f"⚠️ Error processing file: {str(e)}")

if __name__ == "__main__":
    main()
