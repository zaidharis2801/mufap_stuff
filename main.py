# main.py
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from typing import List, Dict, Any
import pandas as pd
import os
import uvicorn # Required to run the FastAPI app

# --- Configuration ---
# Define the base directory where your report folders are located
BASE_DATA_DIR = "." # Current directory, adjust if your folders are elsewhere

REPORT_FOLDERS = {
    "PKISRV": os.path.join(BASE_DATA_DIR, "PKISRV"),
    "PKRV": os.path.join(BASE_DATA_DIR, "PKRV"),
    "PKFRV": os.path.join(BASE_DATA_DIR, "PKFRV")
}

app = FastAPI(
    title="MUFAP Reports Data Viewer",
    description="API to view and access MUFAP CSV report data.",
    version="1.0.0"
)

# --- Helper Function for CSV Reading ---
def read_csv_with_flexible_encoding(filepath: str) -> pd.DataFrame:
    """
    Reads a CSV file, attempting multiple common encodings.
    Skips initial rows that might contain metadata before the actual header.
    """
    encodings_to_try = ['utf-8', 'latin1', 'cp1252']
    
    # Heuristic: Try to find the actual header row if it's not the first row.
    # This is a common issue with reports that have titles/metadata at the top.
    # We'll try reading with different skiprows values.
    for skip_rows in range(5): # Try skipping 0 to 4 rows
        for encoding in encodings_to_try:
            try:
                # Attempt to read, assuming the first non-empty row after skipping is the header
                df = pd.read_csv(filepath, encoding=encoding, skiprows=skip_rows)
                
                # Check if the header looks like actual column names (not just a single long string)
                # If the first column name is a long string or looks like metadata, try skipping more
                if df.empty or len(df.columns) < 2 or (len(df.columns) == 1 and "Unnamed" in df.columns[0]):
                    continue # This header might still be part of metadata, try skipping more
                
                # Drop columns that are entirely unnamed (e.g., 'Unnamed: 0', 'Unnamed: 1')
                # These often result from irregular CSV formatting
                df = df.loc[:, ~df.columns.str.contains('^Unnamed', na=False)]
                
                # Drop rows where all values are NaN (empty rows)
                df.dropna(how='all', inplace=True)

                # Reset index after dropping rows
                df.reset_index(drop=True, inplace=True)

                # If the first column is still problematic, consider it as a sign of bad header detection
                # and continue to the next skip_rows attempt.
                # This is a heuristic and might need fine-tuning for specific files.
                if not df.empty and df.columns[0].strip() == "":
                    continue

                return df
            except pd.errors.EmptyDataError:
                # File is empty or no data after skipping rows
                continue
            except UnicodeDecodeError:
                # Encoding failed, try next encoding
                continue
            except Exception as e:
                # Other pandas reading errors, print and try next encoding/skip_rows
                print(f"Error reading {filepath} with encoding {encoding} and skiprows {skip_rows}: {e}")
                continue
    
    raise ValueError(f"Could not read CSV file: {filepath} with any of the tried encodings/skiprows.")


# --- API Endpoints ---

@app.get("/", response_class=HTMLResponse, summary="Home Page")
async def read_root():
    """
    Provides a simple HTML home page with links to available report types.
    """
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>MUFAP Reports Viewer</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <style>
            body { font-family: 'Inter', sans-serif; }
        </style>
    </head>
    <body class="bg-gray-100 min-h-screen flex flex-col items-center justify-center p-4">
        <div class="bg-white p-8 rounded-lg shadow-lg max-w-md w-full text-center">
            <h1 class="text-3xl font-bold text-gray-800 mb-6">Welcome to MUFAP Reports Viewer</h1>
            <p class="text-gray-600 mb-8">Select a report type to view available files:</p>
            <div class="space-y-4">
                <a href="/reports/PKISRV" class="block bg-blue-500 hover:bg-blue-600 text-white font-semibold py-3 px-6 rounded-lg shadow-md transition duration-300 ease-in-out transform hover:scale-105">
                    View PKISRV Reports
                </a>
                <a href="/reports/PKRV" class="block bg-green-500 hover:bg-green-600 text-white font-semibold py-3 px-6 rounded-lg shadow-md transition duration-300 ease-in-out transform hover:scale-105">
                    View PKRV Reports
                </a>
                <a href="/reports/PKFRV" class="block bg-purple-500 hover:bg-purple-600 text-white font-semibold py-3 px-6 rounded-lg shadow-md transition duration-300 ease-in-out transform hover:scale-105">
                    View PKFRV Reports
                </a>
            </div>
            <p class="text-sm text-gray-500 mt-8">Note: Data consistency varies across files. Pre-processing is recommended for structured analysis.</p>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/reports/{report_type}", response_model=List[str], summary="List Files by Report Type")
async def list_files(report_type: str):
    """
    Lists all available CSV file names for a given report type.
    """
    if report_type not in REPORT_FOLDERS:
        raise HTTPException(status_code=404, detail="Report type not found.")
    
    folder_path = REPORT_FOLDERS[report_type]
    if not os.path.exists(folder_path) or not os.path.isdir(folder_path):
        raise HTTPException(status_code=404, detail=f"Folder for {report_type} not found or is not a directory.")
    
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    if not csv_files:
        raise HTTPException(status_code=404, detail=f"No CSV files found for {report_type}.")
    
    return csv_files

@app.get("/reports/{report_type}/{file_name}", response_model=List[Dict[str, Any]], summary="View Specific Report Data")
async def get_report_data(report_type: str, file_name: str):
    """
    Reads and returns the content of a specific CSV report file as JSON.
    Handles various CSV formats and skips initial metadata rows.
    """
    if report_type not in REPORT_FOLDERS:
        raise HTTPException(status_code=404, detail="Report type not found.")
    
    folder_path = REPORT_FOLDERS[report_type]
    file_path = os.path.join(folder_path, file_name)

    if not os.path.exists(file_path) or not os.path.isfile(file_path):
        raise HTTPException(status_code=404, detail="File not found.")
    
    try:
        df = read_csv_with_flexible_encoding(file_path)
        # Convert DataFrame to a list of dictionaries (JSON format)
        return df.to_dict(orient="records")
    except ValueError as e:
        raise HTTPException(status_code=500, detail=f"Error reading CSV: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

# --- Run the application (for development/testing) ---
# To run this, save it as main.py and execute: uvicorn main:app --reload
# You will need to install uvicorn: pip install uvicorn
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
