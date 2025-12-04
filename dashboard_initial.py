import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

# --- Configuration ---
DB_USER = "user"
DB_PASSWORD = "password"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "finance_db"

# --- Database Connection ---
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

@st.cache_data(ttl=600) # Cache data for 10 minutes
def load_data():
    """Loads and pivots financial data from the database."""
    query = text("""
        WITH pivoted_data AS (
            SELECT
                c.ticker,
                c.name as company_name,
                fs.fiscal_date_ending,
                -- PIVOT the long table into a wide format
                MAX(CASE WHEN fs.metric_name = 'totalRevenue' THEN fs.metric_value ELSE 0 END) AS total_revenue,
                MAX(CASE WHEN fs.metric_name = 'grossProfit' THEN fs.metric_value ELSE 0 END) AS gross_profit,
                MAX(CASE WHEN fs.metric_name = 'operatingIncome' THEN fs.metric_value ELSE 0 END) AS operating_income,
                MAX(CASE WHEN fs.metric_name = 'netIncome' THEN fs.metric_value ELSE 0 END) AS net_income,
                MAX(CASE WHEN fs.metric_name = 'totalCurrentAssets' THEN fs.metric_value ELSE 0 END) AS total_current_assets,
                MAX(CASE WHEN fs.metric_name = 'totalCurrentLiabilities' THEN fs.metric_value ELSE 0 END) AS total_current_liabilities
            FROM financial_statements fs
            JOIN companies c ON fs.company_id = c.id
            WHERE fs.metric_name IN (
                'totalRevenue', 'grossProfit', 'operatingIncome', 'netIncome',
                'totalCurrentAssets', 'totalCurrentLiabilities'
            )
            GROUP BY c.ticker, c.name, fs.fiscal_date_ending
        )
        SELECT * FROM pivoted_data WHERE total_revenue > 0; -- Ensure we only get rows with revenue data
    """)
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
    
    # Convert date and sort
    df['fiscal_date_ending'] = pd.to_datetime(df['fiscal_date_ending'])
    df = df.sort_values(by=['ticker', 'fiscal_date_ending'])
    
    return df

def calculate_metrics(df):
    """Calculates financial metrics from the dataframe."""
    # Replace zeros with NaN to avoid division by zero errors
    df_metrics = df.copy()
    # Use a small number to avoid division by zero, but still calculate for 0 revenue if profit is also 0
    df_metrics['total_revenue'] = df_metrics['total_revenue'].replace(0, 1e-9)

    # Profitability Ratios
    df_metrics['gross_margin_pct'] = (df_metrics['gross_profit'] / df_metrics['total_revenue']) * 100
    df_metrics['operating_margin_pct'] = (df_metrics['operating_income'] / df_metrics['total_revenue']) * 100
    df_metrics['net_margin_pct'] = (df_metrics['net_income'] / df_metrics['total_revenue']) * 100

    # Liquidity Ratio
    df_metrics['current_ratio'] = df_metrics['total_current_assets'] / df_metrics['total_current_liabilities'].replace(0, 1e-9)

    # Growth Metric (Year-over-Year)
    df_metrics['revenue_yoy_growth_pct'] = df_metrics.groupby('ticker')['total_revenue'].pct_change() * 100
    
    # Set fiscal_date_ending as index for easier plotting
    df_metrics.set_index('fiscal_date_ending', inplace=True)
    
    return df_metrics

# --- Streamlit App ---

st.set_page_config(layout="wide", page_title="Financial Dashboard")

st.title("Junior Finance Automation Engineer Take-Home")
st.write("A dashboard analyzing key financial metrics for selected public companies.")

# Load the data from the database
try:
    all_data = load_data()

    if all_data.empty:
        st.error("No financial data found in the database. Please run `main.py` first.")
    else:
        # --- Sidebar for Company Selection ---
        company_list = all_data['ticker'].unique()
        selected_ticker = st.sidebar.selectbox("Select a Company", company_list)
        
        st.sidebar.markdown("---")
        st.sidebar.info("This app fetches financial data from a PostgreSQL database, calculates key metrics, and visualizes them.")

        # Filter data for the selected company
        company_data = all_data[all_data['ticker'] == selected_ticker].copy()
        
        # Calculate metrics for the filtered data
        metrics_df = calculate_metrics(company_data)

        # --- Display Metrics ---
        st.header(f"Financial Analysis for {metrics_df['company_name'].iloc[0]} ({selected_ticker})")

        # Select columns to display
        display_cols = [
            'gross_margin_pct',
            'operating_margin_pct',
            'net_margin_pct',
            'current_ratio',
            'revenue_yoy_growth_pct'
        ]
        
        # Format the dataframe for better readability
        formatted_df = metrics_df[display_cols].T # Transpose for better view
        formatted_df.columns = [d.strftime('%Y-%m-%d') for d in formatted_df.columns]
        
        # *** THIS IS THE CORRECTED FORMATTING LOGIC ***
        for metric_name in formatted_df.index:
            for col in formatted_df.columns:
                value = formatted_df.loc[metric_name, col]
                if pd.isna(value):
                    formatted_df.loc[metric_name, col] = "N/A"
                elif 'pct' in metric_name:
                    formatted_df.loc[metric_name, col] = f"{value:.2f}%"
                else:
                    formatted_df.loc[metric_name, col] = f"{value:.2f}"

        st.subheader("Key Financial Metrics")
        st.table(formatted_df)

        # --- Visualizations ---
        st.subheader("Metric Trends Over Time")
        
        # Profitability Chart
        st.line_chart(metrics_df[['gross_margin_pct', 'operating_margin_pct', 'net_margin_pct']])
        
        # Other charts
        col1, col2 = st.columns(2)
        with col1:
            st.write("Current Ratio (Liquidity)")
            st.bar_chart(metrics_df['current_ratio'])
        with col2:
            st.write("Revenue YoY Growth %")
            st.bar_chart(metrics_df['revenue_yoy_growth_pct'])
        
        # --- Expander for Raw Data ---
        with st.expander("Show Raw Pivoted Data"):
            st.dataframe(company_data)

except Exception as e:
    st.error(f"An error occurred while connecting to the database or loading data: {e}")
    st.info("Please ensure your PostgreSQL database is running (e.g., via `docker-compose up -d`) and that you have run `main.py` successfully.")