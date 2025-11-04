import psycopg2
import pandas as pd
import pathlib
import numpy as np

class postgres:
    def __init__(self,
                 db_name="postgres",
                 user="mauricio",
                 password="12345",
                 host="localhost",
                 port="5432"):
        """Initialize PostgreSQL connection."""
        try:
            self.conn = psycopg2.connect(
                dbname=db_name,
                user=user,
                password=password,
                host=host,
                port=port
            )
            self.conn.autocommit = True
            print(f"✅ Connected successfully to PostgreSQL database: {db_name}")
        except Exception as e:
            print(f"❌ Failed to connect: {e}")

    def fetch_dataframe(self, query):
        """Run a SELECT query and return a DataFrame."""
        try:
            return pd.read_sql_query(query, self.conn)
        except Exception as e:
            print(f"❌ Error executing query: {e}")
            return pd.DataFrame()

    def insert_dataframe(self, df, table_name="invoices"):
        try:
            df = df.where(pd.notnull(df), None)
            df = df.map(lambda x: str(x) if isinstance(x, pathlib.PurePath) else x)

            with self.conn.cursor() as cur:
                cur.execute(f"SELECT invoice_number, supplier_name FROM {table_name}")
                existing = {(i, s) for i, s in cur.fetchall()}

            before_count = len(df)
            df_to_insert = df[
                ~df.apply(lambda x: (x["invoice_number"], x["supplier_name"]) in existing, axis=1)
            ].copy()
            new_count = len(df_to_insert)

            if new_count == 0:
                print("❌ No new invoices to upload — all records already exist.")
                return 0  # <-- no new rows

            with self.conn.cursor() as cur:
                cols = ', '.join(df_to_insert.columns)
                placeholders = ', '.join(['%s'] * len(df_to_insert.columns))
                query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
                for _, row in df_to_insert.iterrows():
                    cur.execute(query, tuple(row))

            self.conn.commit()
            print(f"✅ Inserted {new_count} new rows into '{table_name}' (skipped {before_count - new_count} duplicates).")
            return new_count  # <-- return real number inserted

        except Exception as e:
            print(f"❌ Error inserting data: {e}")
            self.conn.rollback()
            return -1  # <-- indicate failure



    def invoice_exists(self, company_id, invoice_number, invoice_date):
        """Check if an invoice already exists for a given company, date, and invoice number."""
        try:
            query = """
                SELECT COUNT(*) AS count
                FROM invoices
                WHERE company_id = %s
                AND invoice_number = %s
                AND invoice_date = %s
            """
            with self.conn.cursor() as cur:
                cur.execute(query, (company_id, invoice_number, invoice_date))
                result = cur.fetchone()
            return result and result[0] > 0
        except Exception as e:
            print(f"❌ Error checking invoice existence: {e}")
            return False



    def close(self):
        """Close the database connection."""
        self.conn.close()
        print("🔒 Connection closed.")
