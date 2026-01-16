import re
import sqlite3
import os
import pandas as pd
import logging
from datetime import datetime
from typing import List, Tuple, Optional


# Logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    filename="logs/investment_funds.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


class InvestmentFunds:

    def __init__(self, db_name: str = "master_reference.db"):
        self.db_name: str = db_name
        self.fund_names: List[str] = [
            'Whitestone', 'Wallington', 'Catalysm', 'Belaware', 'Gohen',
            'Applebead', 'Magnum', 'Trustmind', 'Leeder', 'Virtous'
        ]
        self.fund_cols: List[str] = [
            'FINANCIAL TYPE', 'SYMBOL', 'SECURITY NAME', 'ISIN',
            'PRICE', 'QUANTITY', 'REALISED P/L', 'MARKET VALUE'
        ]
        self.numeric_cols: List[str] = [
            'PRICE', 'QUANTITY', 'REALISED P/L', 'MARKET VALUE'
        ]

    def get_connection(self):
        return sqlite3.connect(self.db_name)

    def ingest_master_reference(self, sql_file_path: str) -> None:
        # Create the master db using master reference sql

        if not os.path.exists(sql_file_path):
            logging.error(f"SQL file not found: {sql_file_path}")
            return

        with self.get_connection() as conn:
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                conn.executescript(f.read())
        logging.info("Master reference data ingested successfully.")

    def parse_filename(self, filename: str) -> Tuple[Optional[str], Optional[str]]:
        # Get fund name
        fund_name = None
        for name in self.fund_names:
            if name.lower() in filename.lower():
                fund_name = name
                break

        # Look for 8 digits or digits separated by symbols
        date_pattern = r'(\d{8}|\d{2,4}[-._/+\s]\d{2}[-._/+\s]\d{2,4})'
        match = re.search(date_pattern, filename)

        if not match:
            return fund_name, None

        raw_string = match.group(0)
        # Replace all symbols with -
        clean_date_str = re.sub(r'[^0-9]', '-', raw_string)

        # Standardize date
        formats = ('%d-%m-%Y', '%m-%d-%Y', '%Y-%m-%d', '%Y%m%d')
        for format in formats:
            try:
                dt = datetime.strptime(clean_date_str, format)
                # Check we have the right D/M order
                if dt.day >= 28:
                    return fund_name, f"{dt.month}/{dt.day}/{dt.year}"
            except ValueError:
                continue

        print(f"Incorrect date format: {clean_date_str}")
        return fund_name, None

    def validate_dataframe(self, df: pd.DataFrame, fund_name: Optional[str], filename: str) -> bool:

        # Check for missing columns
        missing = [col for col in self.fund_cols if col not in df.columns]
        if missing:
            print(f"{filename} ({fund_name}) is missing columns: {missing}")
            logging.warning(f"{filename} ({fund_name}) is missing columns: {missing}")

        # Check for data types, ensure numeric columns are actually numeric
        for col in self.numeric_cols:
            if not pd.api.types.is_numeric_dtype(df[col]):
                print(f"Data Type Warning: {col} in {filename} is not numeric. Attempting conversion.")
                logging.warning(f"Data Type Warning: {col} in {filename} is not numeric. Attempting conversion.")
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Check for nulls
        if df['PRICE'].isnull().any():
            print(f"{filename} contains NULL prices.")
            logging.warning(f"{filename} contains NULL prices.")

        return True

    def ingest_csv(self) -> None:
        conn = self.get_connection()

        # Create table
        conn.execute('DROP TABLE IF EXISTS "external_funds"')
        conn.execute("""
                    CREATE TABLE IF NOT EXISTS "external_funds" (
                        "FUND NAME" TEXT,
                        "FINANCIAL TYPE" TEXT,
                        "SYMBOL" TEXT,
                        "SEDOL" TEXT,
                        "SECURITY NAME" TEXT,
                        "ISIN" TEXT,
                        "PRICE" REAL,
                        "QUANTITY" REAL,
                        "REALISED P/L" REAL,
                        "MARKET VALUE" REAL,
                        "REPORT DATE" TEXT
                    )
                """)

        csv_folder = "external-funds"
        if not os.path.exists(csv_folder):
            print(f"Error: Folder {csv_folder} not found.")
            return

        files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]

        for file in files:
            try:
                fund_name, report_date = self.parse_filename(file)
                df = pd.read_csv(os.path.join(csv_folder, file))
                print(fund_name, report_date)

                # Run validation
                if not self.validate_dataframe(df, fund_name, file):
                    continue

                df['FUND NAME'] = fund_name
                df['REPORT DATE'] = report_date

                # Load to DB
                df.to_sql('external_funds', conn, if_exists='append', index=False)
                print(f"Ingested {len(df)} records from {fund_name}")
                logging.info(f"Ingested {len(df)} records from {fund_name}")
            except Exception as e:
                logging.error(f"Failed to process {file}: {e}")
                continue

        conn.close()

    def generate_reconciliation_report(self) -> None:
        conn = self.get_connection()

        # Need to read price tables seperately to handle different date formats
        query = """
                    SELECT "ISIN" AS "IDENTIFIER",
                    "PRICE" AS "MASTER_PRICE",
                    "DATETIME"
                    FROM bond_prices
                    """
        bonds = pd.read_sql(query, conn)
        bonds['dt'] = pd.to_datetime(bonds['DATETIME'])

        query = """
                    SELECT "SYMBOL" AS "IDENTIFIER",
                    "PRICE" AS "MASTER_PRICE",
                    "DATETIME"
                    FROM equity_prices
                    """
        equities = pd.read_sql(query, conn)
        equities['dt'] = pd.to_datetime(equities['DATETIME'])

        master_df = pd.concat([bonds, equities],
                              ignore_index=True).sort_values('dt')

        # Merge with fund reports
        query = """
                SELECT *
                FROM external_funds
                """
        fund_df = pd.read_sql(query, conn)
        fund_df['IDENTIFIER'] = fund_df['SYMBOL'].fillna(fund_df['ISIN'])
        fund_df['dt'] = pd.to_datetime(fund_df['REPORT DATE'])
        fund_df = fund_df.sort_values('dt')

        df = pd.merge_asof(
            fund_df,
            master_df[['IDENTIFIER', 'dt', 'MASTER_PRICE']],
            on='dt',
            by='IDENTIFIER',
            direction='backward'
        )

        df['PRICE_DIFF'] = df['PRICE'] - df['MASTER_PRICE']

        os.makedirs('out', exist_ok=True)
        df.to_csv("out/price_reconciliation_analysis.csv", index=False)
        print("Reconciliation report generated.")
        logging.info("Reconciliation report generated.")
        conn.close()

    def generate_performance_report(self) -> None:
        conn = self.get_connection()
        query = """
                    SELECT "FUND NAME",
                    "REPORT DATE",
                    "MARKET VALUE",
                    "REALISED P/L"
                    FROM external_funds
                    """
        df = pd.read_sql(query, conn)
        df['dt'] = pd.to_datetime(df['REPORT DATE'])

        # Get fund market value and realised p/l to calculate ror
        monthly = df.groupby(['FUND NAME', 'dt', 'REPORT DATE']).agg({
            'MARKET VALUE': 'sum',
            'REALISED P/L': 'sum'
        }).reset_index()

        monthly = monthly.sort_values(['FUND NAME', 'dt'])

        # Starting mv as prev month's close
        monthly['STARTING_MV'] = monthly.groupby('FUND NAME')['MARKET VALUE'].shift(1)

        # Given formula
        monthly['ROR'] = (
            (monthly['MARKET VALUE'] - monthly['STARTING_MV'] + monthly['REALISED P/L'])
            / monthly['STARTING_MV']
        )

        # Dropping first month
        monthly = monthly.dropna(subset=['ROR'])

        # Sort by date and ROR to get top fund
        best_funds = monthly.sort_values(['dt', 'ROR'], ascending=[True, False])
        best_funds = best_funds.drop_duplicates('dt')

        best_funds.to_csv("out/best_performing_funds.csv", index=False)
        print("Performance report generated.")
        logging.info("Performance report generated.")
        conn.close()


if __name__ == "__main__":

    analytics = InvestmentFunds()

    sql_file_path = "master-reference-sql.sql"

    # Task 1
    analytics.ingest_master_reference(sql_file_path)
    # Task 2
    analytics.ingest_csv()
    # Task 3
    analytics.generate_reconciliation_report()
    # Task 4
    analytics.generate_performance_report()
