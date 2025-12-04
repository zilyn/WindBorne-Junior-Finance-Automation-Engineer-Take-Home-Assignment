import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import ProgrammingError
import time

# --- Configuration ---
# API Key is hardcoded for testing purposes as requested.
API_KEY = "1K8HTPV7U5KB90ZK"

DB_USER = "user"
DB_PASSWORD = "password"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "finance_db"
COMPANIES = ["TEL", "ST", "DD"]
BASE_URL = "https://www.alphavantage.co/query"

# --- Database Setup ---
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

def setup_database():
    """Creates the database and required tables if they don't exist."""
    print("Setting up database schema...")
    with engine.connect() as connection:
        inspector = inspect(engine)
        if not inspector.has_table("companies"):
            connection.execute(text("""
                CREATE TABLE companies (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) UNIQUE NOT NULL,
                    name VARCHAR(255)
                );
            """))
            connection.commit()
            print("Created 'companies' table.")

        if not inspector.has_table("financial_statements"):
            connection.execute(text("""
                CREATE TABLE financial_statements (
                    id SERIAL PRIMARY KEY,
                    company_id INTEGER REFERENCES companies(id),
                    statement_type VARCHAR(50),
                    fiscal_date_ending DATE,
                    metric_name VARCHAR(255),
                    metric_value BIGINT,
                    UNIQUE(company_id, statement_type, fiscal_date_ending, metric_name)
                );
            """))
            connection.commit()
            print("Created 'financial_statements' table.")
    print("Database setup complete.")


def fetch_company_info(ticker):
    """Fetches company metadata."""
    params = {"function": "OVERVIEW", "symbol": ticker, "apikey": API_KEY}
    print(f"Fetching company info for {ticker}...")
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    time.sleep(15)  # Respect API rate limits
    data = response.json()
    if "Name" in data:
        return {"ticker": ticker, "name": data.get("Name")}
    print(f"Warning: Could not fetch company name for {ticker}. API response: {data}")
    return {"ticker": ticker, "name": ticker} # Fallback

def fetch_financial_statement(ticker, function):
    """Fetches a financial statement for a given company ticker."""
    params = {"function": function, "symbol": ticker, "apikey": API_KEY}
    print(f"Fetching {function} for {ticker}...")
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    time.sleep(15)  # Respect API rate limits
    return response.json()

def transform_and_load(data, company_id, statement_type, connection):
    """Transforms API data into a normalized format and loads it into the database."""
    if "annualReports" not in data:
        print(f"No annual reports found for {statement_type} for company_id {company_id}. API Response: {data}")
        return 0

    # Keep only the last 3 years
    annual_reports = sorted(data["annualReports"], key=lambda x: x['fiscalDateEnding'], reverse=True)[:3]

    records = []
    for report in annual_reports:
        fiscal_date = report["fiscalDateEnding"]
        for key, value in report.items():
            if key != "fiscalDateEnding" and value is not None and value != "None":
                try:
                    numeric_value = int(value)
                    records.append({
                        "company_id": company_id,
                        "statement_type": statement_type,
                        "fiscal_date_ending": fiscal_date,
                        "metric_name": key,
                        "metric_value": numeric_value,
                    })
                except (ValueError, TypeError):
                    continue
    
    if not records:
        return 0

    df = pd.DataFrame(records)
    # Use a temporary table for bulk upsert
    df.to_sql('temp_statements', connection, if_exists='replace', index=False)
    
    # *** THIS IS THE CORRECTED QUERY ***
    upsert_sql = text("""
        INSERT INTO financial_statements (company_id, statement_type, fiscal_date_ending, metric_name, metric_value)
        SELECT t.company_id, t.statement_type, t.fiscal_date_ending::DATE, t.metric_name, t.metric_value
        FROM temp_statements t
        ON CONFLICT (company_id, statement_type, fiscal_date_ending, metric_name) DO NOTHING;
    """)
    result = connection.execute(upsert_sql)
    return result.rowcount

def main():
    """Main function to run the data pipeline."""
    setup_database()
    total_rows_loaded = 0

    with engine.connect() as connection:
        for ticker in COMPANIES:
            print(f"\n--- Processing Ticker: {ticker} ---")
            
            # 1. Get or create the company and retrieve its ID
            company_id = connection.execute(text("SELECT id FROM companies WHERE ticker = :ticker"), {"ticker": ticker}).scalar()
            
            if not company_id:
                company_info = fetch_company_info(ticker)
                result = connection.execute(
                    text("INSERT INTO companies (ticker, name) VALUES (:ticker, :name) RETURNING id"),
                    company_info
                )
                connection.commit()
                company_id = result.scalar()
                print(f"Added company '{company_info.get('name', ticker)}' with ID: {company_id}")
            else:
                print(f"Company {ticker} already exists with ID: {company_id}")

            # 2. Fetch, transform, and load financial data for this company
            statements = {
                "INCOME_STATEMENT": fetch_financial_statement(ticker, "INCOME_STATEMENT"),
                "BALANCE_SHEET": fetch_financial_statement(ticker, "BALANCE_SHEET"),
                "CASH_FLOW": fetch_financial_statement(ticker, "CASH_FLOW"),
            }
            
            for statement_type, data in statements.items():
                # Check for API call limit message
                if 'Information' in data and 'API call frequency' in data['Information']:
                     print(f"API call limit reached. Waiting before retrying... Message: {data['Information']}")
                     time.sleep(60) # Wait for a minute and retry this specific call
                     data = fetch_financial_statement(ticker, statement_type.upper())

                if data and "annualReports" in data:
                    rows = transform_and_load(data, company_id, statement_type, connection)
                    connection.commit()
                    total_rows_loaded += rows
                    print(f"Loaded {rows} new records for {ticker} - {statement_type}")
                else:
                    print(f"Could not load data for {ticker} - {statement_type}. API response: {data.get('Information', data.get('Error Message', 'No data'))}")


    print(f"\nPipeline finished. A total of {total_rows_loaded} new records were loaded.")

if __name__ == "__main__":
    main()