import streamlit as st
import pandas as pd

# The database connection is no longer needed here

@st.cache_data # Cache the data load
def load_data():
    """Loads and pivots financial data from the CSV file."""
    try:
        df_long = pd.read_csv("financial_data.csv")
    except FileNotFoundError:
        st.error("financial_data.csv not found. Please run `export_data.py` first.")
        return pd.DataFrame()

    # Pivot the table to create the wide format needed for calculations
    df = df_long.pivot_table(
        index=['ticker', 'company_name', 'fiscal_date_ending'],
        columns='metric_name',
        values='metric_value'
    ).reset_index()

    # Rename columns to be valid identifiers (e.g., remove spaces)
    df.columns = [col.replace(' ', '_').replace('-', '_') for col in df.columns]

    # Select only the columns we need to avoid errors with missing metrics
    required_cols = [
        'ticker', 'company_name', 'fiscal_date_ending',
        'totalRevenue', 'grossProfit', 'operatingIncome', 'netIncome',
        'totalCurrentAssets', 'totalCurrentLiabilities'
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = 0 # Add missing columns and fill with 0

    df = df[required_cols]

    # Convert date and sort
    df['fiscal_date_ending'] = pd.to_datetime(df['fiscal_date_ending'])
    df = df.sort_values(by=['ticker', 'fiscal_date_ending'])

    # Ensure data types are correct
    for col in ['totalRevenue', 'grossProfit', 'operatingIncome', 'netIncome', 'totalCurrentAssets', 'totalCurrentLiabilities']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    return df

def calculate_metrics(df):
    """Calculates financial metrics from the dataframe."""
    df_metrics = df.copy()
    
    # Use a small number to avoid division by zero errors
    df_metrics['totalRevenue'] = df_metrics['totalRevenue'].replace(0, 1e-9)
    df_metrics['totalCurrentLiabilities'] = df_metrics['totalCurrentLiabilities'].replace(0, 1e-9)

    # Profitability Ratios
    df_metrics['gross_margin_pct'] = (df_metrics['grossProfit'] / df_metrics['totalRevenue']) * 100
    df_metrics['operating_margin_pct'] = (df_metrics['operatingIncome'] / df_metrics['totalRevenue']) * 100
    df_metrics['net_margin_pct'] = (df_metrics['netIncome'] / df_metrics['totalRevenue']) * 100

    # Liquidity Ratio
    df_metrics['current_ratio'] = df_metrics['totalCurrentAssets'] / df_metrics['totalCurrentLiabilities']

    # Growth Metric (Year-over-Year)
    df_metrics['revenue_yoy_growth_pct'] = df_metrics.groupby('ticker')['totalRevenue'].pct_change() * 100
    
    df_metrics.set_index('fiscal_date_ending', inplace=True)
    
    return df_metrics

# --- Streamlit App ---

st.set_page_config(layout="wide", page_title="Financial Dashboard")
st.title("Junior Finance Automation Engineer Take-Home")
st.write("A dashboard analyzing key financial metrics for selected public companies.")

all_data = load_data()

if all_data.empty:
    st.warning("Could not load financial data.")
else:
    company_list = all_data['ticker'].unique()
    selected_ticker = st.sidebar.selectbox("Select a Company", company_list)
    
    st.sidebar.markdown("---")
    st.sidebar.info("This app fetches financial data from a pre-processed CSV file and visualizes key metrics.")

    company_data = all_data[all_data['ticker'] == selected_ticker].copy()
    
    metrics_df = calculate_metrics(company_data)

    st.header(f"Financial Analysis for {metrics_df['company_name'].iloc[0]} ({selected_ticker})")

    display_cols = [
        'gross_margin_pct',
        'operating_margin_pct',
        'net_margin_pct',
        'current_ratio',
        'revenue_yoy_growth_pct'
    ]
    
    formatted_df = metrics_df[display_cols].T
    formatted_df.columns = [d.strftime('%Y-%m-%d') for d in formatted_df.columns]
    
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

    st.subheader("Metric Trends Over Time")
    
    st.line_chart(metrics_df[['gross_margin_pct', 'operating_margin_pct', 'net_margin_pct']])
    
    col1, col2 = st.columns(2)
    with col1:
        st.write("Current Ratio (Liquidity)")
        st.bar_chart(metrics_df['current_ratio'])
    with col2:
        st.write("Revenue YoY Growth %")
        st.bar_chart(metrics_df['revenue_yoy_growth_pct'])
    
    with st.expander("Show Raw Pivoted Data"):
        st.dataframe(company_data)