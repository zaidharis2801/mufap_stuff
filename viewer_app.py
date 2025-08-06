import pandas as pd
import sqlite3
import os
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from jinja2 import Template

# --- Configuration ---
DATABASE_PATH = "financial_data.db"
# This is the base directory where your PKFRV/PKRV folders are located.
# The app needs this to construct the correct download paths.
FILES_BASE_DIRECTORY = "." 

# --- FastAPI App Initialization ---
app = FastAPI(
    title="Financial Data Viewer",
    description="A modern web interface to view and download financial data.",
    version="1.0.0", port =7484
)

# --- HTML Templates ---
# Using Jinja2 templates directly in the script for simplicity.

HTML_TEMPLATE = Template("""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Financial Data Viewer</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Inter', sans-serif; }
        .table-container { max-height: 80vh; }
    </style>
</head>
<body class="bg-gray-50 text-gray-800">

    <div class="container mx-auto p-4 sm:p-6 lg:p-8">
        <header class="text-center mb-8">
            <h1 class="text-4xl font-bold text-gray-900">Financial Data Viewer</h1>
            <p class="text-lg text-gray-600 mt-2">Select a report type to view the data.</p>
        </header>

        <div class="flex justify-center gap-4 mb-8">
            <a href="/data/PKFRV" class="bg-blue-600 hover:bg-blue-700 text-white font-bold py-3 px-6 rounded-lg shadow-md transition-transform transform hover:scale-105">
                View Mutual Fund Data (PKFRV)
            </a>
            <a href="/data/PKRV" class="bg-green-600 hover:bg-green-700 text-white font-bold py-3 px-6 rounded-lg shadow-md transition-transform transform hover:scale-105">
                View Tenor Rates (PKRV)
            </a>
        </div>

        {% if data %}
        <div class="bg-white rounded-xl shadow-lg overflow-hidden">
            <div class="p-6 border-b border-gray-200">
                <h2 class="text-2xl font-semibold">Displaying {{ table_name }} Data</h2>
            </div>
            <div class="table-container overflow-x-auto">
                <table class="min-w-full divide-y divide-gray-200">
                    <thead class="bg-gray-50 sticky top-0">
                        <tr>
                            {% for header in headers %}
                            <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                {{ header }}
                            </th>
                            {% endfor %}
                            <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Action
                            </th>
                        </tr>
                    </thead>
                    <tbody class="bg-white divide-y divide-gray-200">
                        {% for row in data %}
                        <tr>
                            {% for cell in row %}
                            <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-700">
                                {{ cell }}
                            </td>
                            {% endfor %}
                            <td class="px-6 py-4 whitespace-nowrap text-sm">
                                <a href="/download/{{ row[-1] }}" class="text-indigo-600 hover:text-indigo-900 font-medium" download>
                                    Download File
                                </a>
                            </td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </div>
        {% endif %}
    </div>

</body>
</html>
""")

# --- Helper Functions ---

def db_connect():
    """Establishes a connection to the SQLite database."""
    if not os.path.exists(DATABASE_PATH):
        raise HTTPException(status_code=500, detail=f"Database file not found at '{DATABASE_PATH}'. Please run the processing script first.")
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        return conn
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {e}")

def get_clean_ordered_data(report_type: str):
    """
    Fetches data from the database, cleans unwanted columns, and orders them.
    """
    conn = db_connect()
    
    if report_type == "PKFRV":
        table_name = "mutual_fund_data"
        # Define the specific order for PKFRV columns
        fixed_order = ['Report_Date', 'Issue Date', 'Maturity Date', 'Coupon Frequency']
    elif report_type == "PKRV":
        table_name = "tenor_rates"
        # Define the specific order for PKRV columns
        fixed_order = ['report_date', 'Tenor', 'Mid Rate', 'Change']
    else:
        raise HTTPException(status_code=404, detail="Report type not found.")

    try:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    except pd.io.sql.DatabaseError:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in the database.")
    finally:
        conn.close()

    # Store the source filepath for the download link, then drop it
    source_filepaths = df['Source_Filepath'].copy() if 'Source_Filepath' in df.columns else df['source_filepath'].copy()

    # --- Column Filtering ---
    # List of columns to always exclude
    cols_to_exclude = {'unique_id', 'Source_Filepath', 'source_filepath', 'Report_Date', 'report_date', '﻿'}
    # Dynamically find 'Unnamed' columns
    unnamed_cols = {col for col in df.columns if 'unnamed' in col.lower()}
    cols_to_exclude.update(unnamed_cols)
    
    # Get the remaining data columns
    data_cols = [col for col in df.columns if col not in cols_to_exclude]

    # --- Column Ordering ---
    # Start with the fixed order, then add the rest of the data columns alphabetically
    final_ordered_cols = []
    for col in fixed_order:
        if col in df.columns:
            final_ordered_cols.append(col)
    
    remaining_cols = sorted([col for col in data_cols if col not in final_ordered_cols])
    final_ordered_cols.extend(remaining_cols)

    # Create the final DataFrame with ordered columns
    final_df = df[final_ordered_cols]
    
    # Add the source filepath back as the last column for the download link
    final_df['source_filepath_for_download'] = source_filepaths
    
    return final_df.values.tolist(), final_ordered_cols, table_name.replace('_', ' ').title()


# --- API Endpoints ---

@app.get("/", response_class=HTMLResponse)
async def read_root():
    """
    Serves the main landing page with buttons to select a report type.
    """
    return HTML_TEMPLATE.render()

@app.get("/data/{report_type}", response_class=HTMLResponse)
async def get_report_data(report_type: str):
    """
    Fetches, cleans, and displays data for the selected report type.
    """
    data, headers, table_name = get_clean_ordered_data(report_type.upper())
    return HTML_TEMPLATE.render(data=data, headers=headers, table_name=table_name)

@app.get("/download/{filepath:path}")
async def download_file(filepath: str):
    """
    Provides a file for download. The path is constructed relative to the base directory.
    """
    # Construct the full, safe path to the file
    full_path = os.path.join(FILES_BASE_DIRECTORY, filepath)
    
    if not os.path.exists(full_path):
        raise HTTPException(status_code=404, detail="File not found.")
        
    return FileResponse(path=full_path, filename=os.path.basename(full_path))

# To run this app:
# 1. Save it as viewer_app.py
# 2. Make sure financial_data.db is in the same directory.
# 3. Make sure your PKFRV and PKRV folders are also in the same directory.
# 4. Run in your terminal: uvicorn viewer_app:app --reload
