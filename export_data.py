import pandas as pd
from sqlalchemy import create_engine, text

# --- Configuration ---
DB_USER = "user"
DB_PASSWORD = "password"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "finance_db"
OUTPUT_FILE = "financial_data.csv"

# --- Database Connection ---
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

def export_data():
    """Loads financial data from the database and saves it to a CSV."""
    query = text("""
        SELECT
            c.ticker,
            c.name as company_name,
            fs.fiscal_date_ending,
            fs.metric_name,
            fs.metric_value
        FROM financial_statements fs
        JOIN companies c ON fs.company_id = c.id
    """)
    print("Connecting to database and fetching data...")
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)

    print(f"Successfully fetched {len(df)} rows.")
    
    # Save to CSV
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Data successfully saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    export_data()