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

st.markdown("---")
st.header("Part 4: Productionizing the Pipeline")
st.markdown(
    """
### Architectural Note: Local Development vs. Public Deployment

This project was built with a clear distinction between the local development environment and the deployed public application:

*   **Local Development:** The core data pipeline (`main.py`) is designed to run locally. It fetches data from the Alpha Vantage API, transforms it, and loads it into a normalized **PostgreSQL database** managed via Docker. The initial version of this dashboard connected directly to this live database, proving the full ETL and database design works as intended.

*   **Public Deployment:** To meet the requirement of a public URL, a pragmatic adjustment was made. Since a web app on Streamlit Community Cloud cannot access a local database, the data from the populated database was exported to a static `financial_data.csv` file. **The deployed dashboard reads from this static CSV.** This decision decouples the presentation layer from the database, ensuring the public app is fast, reliable, and simple to deploy, while still demonstrating that the full database pipeline was successfully built.

---

### How would you schedule your code to run monthly?

For the given tech stack (**n8n, PostgreSQL, Google Sheets**), the best choice is to use **n8n** for scheduling. A cron job is a valid alternative, but n8n provides better visibility, error handling, and integration capabilities for this specific stack.

A simple n8n workflow would look like this:

**Workflow Diagram (Conceptual):**

```
[Start: Schedule Trigger (Monthly)]
          |
          v
[Execute Python Script: main.py] --> [On Failure: Send Alert (Email/Slack)]
          |
          v
[On Success: Send Success Notification (Email)]
```

**Pseudocode for the n8n "Execute Command" Node:**

This node would run the Python script responsible for the ETL process.

```bash
# 1. Activate the Python virtual environment
source /path/to/venv/bin/activate

# 2. Navigate to the project directory
cd /path/to/project

# 3. Run the main data pipeline script
python main.py

# 4. Check the exit code of the script
if [ $? -eq 0 ]; then
  echo "Pipeline completed successfully."
  # n8n will proceed down the 'success' path
else
  echo "Pipeline failed."
  # n8n will proceed down the 'failure' path, triggering an alert
  exit 1
fi
```

---

### How would you handle the API rate limit for 100 companies?

The free Alpha Vantage API limit (25 calls/day) makes it impossible to fetch data for 100 companies in a single run (100 companies * 3 statements/company = 300 calls needed).

The solution is to design an **incremental and stateful pipeline** that runs daily and processes a small batch of companies each time.

**Changes Required:**

1.  **Modify the `companies` table:** Add a `last_synced_at` timestamp column.
    ```sql
    ALTER TABLE companies ADD COLUMN last_synced_at TIMESTAMP;
    ```
2.  **Modify the Python script (`main.py`):**
    *   Instead of processing a hardcoded list of companies, the script will query the `companies` table.
    *   It will select a small batch (e.g., 8 companies) with the oldest `last_synced_at` dates. 8 companies * 3 statements = 24 calls, which is safely within the 25/day limit.
    *   After successfully fetching and loading data for a company, the script will update its `last_synced_at` timestamp in the database.

3.  **Update the n8n Workflow:** Change the schedule trigger from "monthly" to "**daily**".

This creates a rolling update cycle. Over ~12 days (100 / 8), all companies will have their data refreshed. The system is resilient and automatically works through the backlog.

---

### How would execs access this data in Google Sheets?

Given the options, the best approach is to **sync the data from PostgreSQL to Google Sheets via n8n**, avoiding a direct database connection.

**My choice:** Use n8n's built-in PostgreSQL and Google Sheets nodes.

**Justification (Pros & Cons):**

1.  **Direct Postgres Connection to Google Sheets:**
    *   **Pros:** Data is always live.
    *   **Cons:** **(Major Security Risk)** Requires exposing the database to the public internet and storing credentials in a Google Sheet extension, which is a significant security vulnerability. Also complex for non-technical execs to set up and maintain. **This is not recommended.**

2.  **Export to CSV and Manual Import:**
    *   **Pros:** Simple and secure.
    *   **Cons:** A completely manual process that undermines the goal of automation. Prone to human error and creates stale data.

3.  **Sync via n8n (Recommended Approach):**
    *   **Pros:**
        *   **Automated:** The sync runs automatically after the data pipeline finishes.
        *   **Secure:** The database credentials live securely within n8n, not exposed to the public or to end-users.
        *   **Easy for Execs:** The data simply appears in their Google Sheet. No setup is required on their end.
        *   **Structured:** n8n can be configured to push only the most important, pre-calculated metrics, not the entire raw database.
    *   **Cons:** Data is only as fresh as the last sync. For monthly financial data, this is not a significant issue.

---

### What breaks first and how do you know?

**1. What Breaks First: The Alpha Vantage API Integration.**
This is the most fragile part of the system because it's an external dependency we don't control. It can fail in several ways:
*   **API Key Issues:** The key expires or is invalidated.
*   **Endpoint Changes:** Alpha Vantage changes a metric name (e.g., `totalRevenue` becomes `revenueTotal`). Our code would fail to parse this.
*   **Bad Data:** The API returns a `200 OK` status but the data inside is corrupted, null, or formatted incorrectly (e.g., a number is returned as `"--"`). Our `int()` conversion would crash.
*   **Rate Limiting:** Our logic doesn't correctly handle the rate limit, causing subsequent calls to fail.

**2. How Do You Know? Monitoring and Alerts.**
You know it's broken when an alert fires. We need to add monitoring at key stages:
*   **n8n Workflow Alerts:** Configure the workflow to send an email or Slack message immediately upon any node failure (e.g., the Python script returning an error). The alert should contain the error logs.
*   **Application-Level Logging:** Enhance the Python script to log detailed information. If a specific company fails, log the ticker and the error so it can be investigated without stopping the entire pipeline.
*   **"Dead Man's Snitch":** A simple but powerful concept. Have the n8n workflow send a "heartbeat" to a monitoring service (like Healthchecks.io) on every successful run. If that heartbeat doesn't arrive on schedule, the monitoring service sends an alert. This catches "silent failures" where the workflow doesn't run at all.

**3. How Do You Detect Bad Data?**
This requires adding a **data validation and cleaning layer** after fetching data but before loading it.
*   **Schema Validation:** Use a library like `Pandas` or `Pydantic` to check that the API response contains the expected fields and data types. If `totalRevenue` is expected to be a number but comes back as a string, flag it.
*   **Sanity Checks (Anomaly Detection):** Implement business logic rules. For example, check that `totalRevenue` is not negative or that `grossProfit` is not greater than `totalRevenue`.
*   **Quarantine and Alert:** If a record fails validation, don't load it into the main table. Move it to a separate `quarantined_records` table in the database and fire a specific "Bad Data Detected" alert with the problematic data. This prevents data corruption and allows for manual review.
"""
)
