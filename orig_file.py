import re
import sqlite3
import os
import pandas as pd
import logging
from datetime import datetime


os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    filename="logs/investment_funds.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


db_name = "master_reference.db"

fund_names = [
    'Whitestone', 'Wallington', 'Catalysm', 'Belaware', 'Gohen',
    'Applebead', 'Magnum', 'Trustmind', 'Leeder', 'Virtous'
]


def get_connection():
    return sqlite3.connect(db_name)


def ingest_master_reference(sql_file_path, db_name=db_name):

    if not os.path.exists(sql_file_path):
        print(f"Error: The file '{sql_file_path}' was not found.")
        logging.error(f"Error: The file '{sql_file_path}' was not found.")
        return

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    print(f"Connecting to database: {db_name}...")
    logging.info(f"Connecting to database: {db_name}...")

    try:

        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()

        print("Executing SQL script to initialize tables and insert data...")
        logging.info("Executing SQL script to initialize tables and insert data...")
        cursor.executescript(sql_script)
        conn.commit()
        print("Ingestion complete.")
        logging.info("Ingestion complete.")

    except Exception as e:
        print(f"An error occurred: {e}")
        logging.error(f"An error occurred: {e}")
    finally:
        conn.close()
        print("Database connection closed.")
        logging.info("Database connection closed.")


def parse_filename(filename):

    # Get fund name
    for name in fund_names:
        if name.lower() in filename.lower():
            fund_name = name
            break

    # Look for 8 digits or digits separated by symbols
    date_pattern = r'(\d{8}|\d{2,4}[-._/+\s]\d{2}[-._/+\s]\d{2,4})'
    match = re.search(date_pattern, filename)

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


def validate_dataframe(df, fund_name, filename):
    fund_cols = ['FINANCIAL TYPE', 'SYMBOL', 'SECURITY NAME', 'ISIN', 'PRICE',
                 'QUANTITY', 'REALISED P/L', 'MARKET VALUE']

    # Check for missing columns
    missing = [col for col in fund_cols if col not in df.columns]
    if missing:
        print(f"{filename} ({fund_name}) is missing columns: {missing}")
        logging.warning(f"{filename} ({fund_name}) is missing columns: {missing}")

    # Check for data types: ensure numeric columns are actually numeric
    numeric_cols = ['PRICE', 'QUANTITY', 'REALISED P/L', 'MARKET VALUE']
    for col in numeric_cols:
        if not pd.api.types.is_numeric_dtype(df[col]):
            print(f"Data Type Warning: {col} in {filename} is not numeric. Attempting conversion.")
            logging.warning(f"Data Type Warning: {col} in {filename} is not numeric. Attempting conversion.")
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Check for nulls
    if df['PRICE'].isnull().any():
        print(f"{filename} contains NULL prices.")
        logging.warning(f"{filename} contains NULL prices.")

    return True


def ingest_csv():
    conn = get_connection()

    # Create table
    conn.execute('DROP TABLE IF EXISTS "external_funds"')
    conn.execute("""
                CREATE TABLE IF NOT EXISTS "external_funds" (
                    "FUND NAME" TEXT,
                    "FINANCIAL TYPE" TEXT,
                    "SYMBOL" TEXT,
                    "SEDOL"	TEXT,
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
    files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]

    for file in files:
        fund_name, report_date = parse_filename(file)
        df = pd.read_csv(os.path.join(csv_folder, file))
        print(fund_name, report_date)

        # Run validation
        if not validate_dataframe(df, fund_name, file):
            continue

        df['FUND NAME'] = fund_name
        df['REPORT DATE'] = report_date

        # Load to DB
        df.to_sql('external_funds', conn, if_exists='append', index=False)
        print(f"Ingested {len(df)} records from {fund_name}")
        logging.info(f"Ingested {len(df)} records from {fund_name}")


def generate_reconciliation_report():

    conn = get_connection()

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

    df.to_csv("out/price_reconciliation_analysis_2.csv", index=False)
    print("Reconciliation report generated.")
    logging.info("Reconciliation report generated.")
    conn.close()


def generate_performance_report():

    conn = get_connection()
    query = """
                SELECT "FUND NAME",
                "REPORT DATE",
                "MARKET VALUE",
                "REALISED P/L"
                FROM external_funds
                """
    df = pd.read_sql(query, conn)
    df['dt'] = pd.to_datetime(df['REPORT DATE'])

    monthly = df.groupby(['FUND NAME', 'dt', 'REPORT DATE']).agg({
        'MARKET VALUE': 'sum',
        'REALISED P/L': 'sum'
    }).reset_index()

    monthly = monthly.sort_values(['FUND NAME', 'dt'])

    # Starting mv as prev month's close
    monthly['STARTING_MV'] = monthly.groupby('FUND NAME')['MARKET VALUE'].shift(1)

    monthly['ROR'] = (
        (monthly['MARKET VALUE'] - monthly['STARTING_MV'] + monthly['REALISED P/L'])
        / monthly['STARTING_MV']
    )

    # Dropping first month
    monthly = monthly.dropna(subset=['ROR'])

    # Sort by date and ROR to get top fund
    best_funds = monthly.sort_values(['dt', 'ROR'], ascending=[True, False])
    best_funds = best_funds.drop_duplicates('dt')

    # query = """
    # WITH monthly_totals AS (
    #     SELECT
    #         "FUND NAME",
    #         "REPORT DATE",
    #         SUM("MARKET VALUE") as total_mv,
    #         SUM("REALISED P/L") as total_pl
    #     FROM "external_funds"
    #     GROUP BY "FUND NAME", "REPORT DATE"
    # ),
    # performance_calc AS (
    #     SELECT
    #         *,
    #         LAG(total_mv) OVER (PARTITION BY "FUND NAME" ORDER BY "REPORT DATE") as starting_mv
    #     FROM monthly_totals
    # )
    # SELECT
    #     "REPORT DATE",
    #     "FUND NAME",
    #     total_mv as "ENDING_MARKET_VALUE",
    #     starting_mv as "STARTING_MARKET_VALUE",
    #     total_pl as "REALISED_PL",
    #     ((total_mv - starting_mv + total_pl) /
    #      NULLIF(starting_mv, 0)) as "ROR"
    # FROM performance_calc
    # WHERE starting_mv IS NOT NULL
    # ORDER BY "REPORT DATE", "ROR" DESC
    # """

    # df = pd.read_sql(query, conn)

    # # Step 4: Identify the #1 fund for each month
    # best_funds = df.sort_values('ROR', ascending=False).drop_duplicates('REPORT DATE')

    best_funds.to_csv("out/best_performing_funds.csv", index=False)
    conn.close()
    # print("Performance report generated.")
    logging.info("Performance report generated.")


if __name__ == "__main__":
    sql_file_path = "master-reference-sql.sql"
    # Reference data setup
    ingest_master_reference(sql_file_path)

    # Import CSV data to database
    ingest_csv()

    # Generate performance report
    generate_reconciliation_report()

    # Generate best performing fund report
    generate_performance_report()
