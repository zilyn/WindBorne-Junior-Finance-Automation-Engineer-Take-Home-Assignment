# WindBorne-Junior-Finance-Automation-Engineer-Take-Home-Assignment

This repository contains the solution for the WindBorne Junior Finance Automation Engineer take-home assignment. The project involves fetching financial data from the Alpha Vantage API, storing it in a PostgreSQL database, performing financial analysis, and displaying the results on a public dashboard.

---

### **Live Dashboard URL**

The live dashboard is deployed on Streamlit Community Cloud and can be accessed here:

**[https://windborne-junior-finance-automation-engineer-take-home-assignm.streamlit.app/](https://windborne-junior-finance-automation-engineer-take-home-assignm.streamlit.app/)**

---

### Tech Stack

*   **Programming Language:** Python 3.11
*   **Data Fetching:** Alpha Vantage API, `requests`
*   **Database:** PostgreSQL (managed with Docker)
*   **ETL & Data Transformation:** `pandas`, `SQLAlchemy`
*   **Dashboard & Visualization:** `streamlit`
*   **Automation & Scheduling (Proposed):** n8n

---

### Local Development Setup

To run this project on your local machine, follow these steps:

**Prerequisites:**
*   Python 3.11 or later
*   Docker and Docker Compose

**Instructions:**

1.  **Clone the Repository:**
    ```bash
    git clone <your-repo-url>
    cd <your-repo-name>
    ```

2.  **Install Python Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Set Up API Key:**
    *   Create a file named `.env` in the root directory.
    *   Add your Alpha Vantage API key to it:
      ```
      ALPHA_VANTAGE_API_KEY="YOUR_API_KEY"
      ```
    *(Note: For the purpose of this assignment, the key is also hardcoded in `main.py` to ensure functionality if the `.env` file is missed, but using environment variables is the standard best practice).*

4.  **Start the PostgreSQL Database:**
    *   Make sure Docker Desktop is running.
    *   Run the following command to start the database container in the background:
      ```bash
      docker-compose up -d
      ```

5.  **Run the Data Pipeline:**
    *   Execute the main script to fetch data from the API and load it into the database. This will take a few minutes due to API rate limiting.
      ```bash
      python main.py
      ```

6.  **Run the Local Dashboards:**
    This project contains two versions of the dashboard for demonstration:
    
    *   **To run the original dashboard connected to the live PostgreSQL database:**
        ```bash
        python -m streamlit run dashboard_initial.py
        ```

    *   **To run the final, deployed version that reads from the CSV file:**
        *(This version reads from `financial_data.csv`, which is included in the repository).*
        ```bash
        python -m streamlit run dashboard.py
        ```

---

### Project File Structure

*   `main.py`: The core ETL script. Fetches data from Alpha Vantage and loads it into the PostgreSQL database.
*   `dashboard_initial.py`: The **local development** Streamlit app that connects directly to the PostgreSQL database.
*   `dashboard.py`: The **final deployed** Streamlit app that reads data from the pre-processed `financial_data.csv`.
*   `export_data.py`: A utility script to export data from the PostgreSQL database to `financial_data.csv`.
*   `docker-compose.yml`: Defines and configures the PostgreSQL service for local development.
*   `requirements.txt`: A list of all Python dependencies.
*   `runtime.txt`: Specifies the Python version for Streamlit Cloud deployment.
*   `.env`: Stores the secret API key (not committed to Git).
