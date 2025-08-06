import requests
import json
import sqlite3
import os
import re
from datetime import datetime
import asyncio
import aiohttp # For asynchronous HTTP requests
import aiofiles # For asynchronous file operations (optional, but good practice with asyncio)

# --- Configuration ---
API_URL = "https://mufap.com.pk/WebRegulations/GetSecpFileById"
BASE_FILE_DOWNLOAD_URL = "https://mufap.com.pk"
DATABASE_NAME = "mufap_data.db"
REPORT_FOLDERS = {
    "PKISRV": "PKISRV",
    "PKRV": "PKRV",
    "PKFRV": "PKFRV"
}
# Ensure these folders exist
for folder in REPORT_FOLDERS.values():
    os.makedirs(folder, exist_ok=True)

# --- Database Functions ---
# SQLite operations are synchronous, but we'll manage connections carefully.
def connect_db():
    """Establishes a connection to the SQLite database."""
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        conn.row_factory = sqlite3.Row # Allows accessing columns by name
        return conn
    except sqlite3.Error as e:
        print(f"Database connection error: {e}")
        return None

def create_table(conn):
    """Creates the mufap_reports table if it doesn't exist."""
    try:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS mufap_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                timeofadding DATETIME DEFAULT CURRENT_TIMESTAMP,
                date TEXT,
                title TEXT,
                filepath TEXT UNIQUE, -- Added UNIQUE constraint for FilePath
                report_type TEXT,
                fk_header_submenu_tab_id INTEGER
            )
        ''')
        conn.commit()
        print("Table 'mufap_reports' ensured to exist.")
    except sqlite3.Error as e:
        print(f"Error creating table: {e}")

def report_exists_in_db(conn, filepath):
    """Checks if a report with the given filepath already exists in the database."""
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM mufap_reports WHERE filepath = ?", (filepath,))
    return cursor.fetchone() is not None

def insert_report_data(conn, report_data):
    """Inserts a single report's metadata into the database, preventing duplicates."""
    filepath = report_data.get('FilePath')
    if not filepath:
        print(f"Skipping insertion for report with no FilePath: {report_data.get('Title')}")
        return False

    if report_exists_in_db(conn, filepath):
        print(f"Skipping DB insertion: Report already exists for FilePath: {filepath}")
        return False

    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO mufap_reports (date, title, filepath, report_type, fk_header_submenu_tab_id)
            VALUES (?, ?, ?, ?, ?)
        ''', (report_data['Date'], report_data['Title'], report_data['FilePath'], report_data['report_type'], report_data['fk_HeaderSubMenuTabId']))
        conn.commit()
        print(f"Inserted: {report_data['Title']} (fk_HeaderSubMenuTabId: {report_data['fk_HeaderSubMenuTabId']})")
        return True
    except sqlite3.Error as e:
        print(f"Error inserting data for {report_data['Title']}: {e}")
        return False

# --- Helper Functions ---
def parse_dotnet_date(date_string):
    """
    Parses a .NET /Date(...) string and returns a formatted datetime string.
    Example: /Date(1580238000000)/ -> 2020-01-28 11:00:00
    """
    match = re.search(r'\/Date\((\d+)\)\/', date_string)
    if match:
        timestamp_ms = int(match.group(1))
        # Convert milliseconds to seconds
        dt_object = datetime.fromtimestamp(timestamp_ms / 1000)
        return dt_object.strftime('%Y-%m-%d %H:%M:%S')
    return date_string # Return original if not in expected format

def get_report_type(title):
    """Determines the report type based on the title."""
    title_upper = title.upper()
    if "PKISRV" in title_upper:
        return "PKISRV"
    elif "PKRV" in title_upper:
        return "PKRV"
    elif "PKFRV" in title_upper:
        return "PKFRV"
    return "UNKNOWN"

async def download_file(session, file_url, save_path):
    """Downloads a file from a given URL and saves it to a specified path asynchronously."""
    if os.path.exists(save_path):
        print(f"Skipping download: File already exists at: {save_path}")
        return True

    try:
        print(f"Attempting to download: {file_url}")
        async with session.get(file_url) as response:
            response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)

            # Using aiofiles for asynchronous file writing
            async with aiofiles.open(save_path, 'wb') as f:
                async for chunk in response.content.iter_chunked(8192):
                    await f.write(chunk)
        print(f"Successfully downloaded and saved to: {save_path}")
        return True
    except aiohttp.ClientError as e:
        print(f"Error downloading {file_url}: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during download of {file_url}: {e}")
        return False

async def process_single_report(session, conn, report_data_raw, fk_header_submenu_tab_id):
    """Processes a single report: parses data, inserts into DB, and downloads file."""
    # Create a mutable copy of report_data_raw
    report = report_data_raw.copy()

    # Process metadata
    report['Date'] = parse_dotnet_date(report.get('Date', ''))
    report['report_type'] = get_report_type(report.get('Title', ''))
    report['fk_HeaderSubMenuTabId'] = fk_header_submenu_tab_id # Add the ID to the report data

    # Insert into DB (synchronous, but fast enough for individual inserts)
    if insert_report_data(conn, report):
        # Download file
        file_path_suffix = report.get('FilePath')
        if file_path_suffix:
            full_file_url = BASE_FILE_DOWNLOAD_URL + file_path_suffix
            
            # Determine save directory
            report_type_folder = REPORT_FOLDERS.get(report['report_type'], 'UNKNOWN_REPORTS')
            os.makedirs(report_type_folder, exist_ok=True) # Ensure folder exists
            
            # Extract filename from FilePath
            file_name = os.path.basename(file_path_suffix)
            save_path = os.path.join(report_type_folder, file_name)
            
            await download_file(session, full_file_url, save_path)
        else:
            print(f"No FilePath found for report: {report.get('Title')}")
    else:
        print(f"Skipping file download for {report.get('Title')} due to DB insertion being skipped or failed.")


# --- Main Scraper Logic ---
async def scrape_mufap_reports(fk_header_submenu_tab_id):
    """
    Main asynchronous function to fetch reports, store metadata in DB, and download files.
    Accepts fk_header_submenu_tab_id as an argument.
    """
    conn = connect_db()
    if not conn:
        return

    create_table(conn)

    headers = {
        'Content-Type': 'application/json',
        'Referer': 'https://mufap.com.pk/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    }
    payload = {'fk_HeaderSubMenuTabId': fk_header_submenu_tab_id}

    print(f"\n--- Fetching data for fk_HeaderSubMenuTabId: {fk_header_submenu_tab_id} ---")
    
    async with aiohttp.ClientSession(headers=headers) as session:
        try:
            async with session.post(API_URL, json=payload) as response:
                response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
                
                data_response = await response.json()

            # Check for the "No data found" message but continue to process the 'data' field
            if data_response.get("statusCode") == "00" and data_response.get("message") == "No data found":
                print(f"API returned 'No data found' message for fk_HeaderSubMenuTabId: {fk_header_submenu_tab_id}. Still checking 'data' field...")

            reports = data_response.get("data", [])
            if not reports:
                print(f"No reports found in the 'data' field of the response for fk_header_submenu_tab_id: {fk_header_submenu_tab_id}.")
                return # Exit if 'data' is truly empty

            print(f"Found {len(reports)} reports for fk_header_submenu_tab_id: {fk_header_submenu_tab_id}.")
            
            # Create a list of tasks to run concurrently
            tasks = []
            for report_data_raw in reports:
                tasks.append(process_single_report(session, conn, report_data_raw, fk_header_submenu_tab_id))
            
            # Run all tasks concurrently
            await asyncio.gather(*tasks)

        except aiohttp.ClientError as e:
            print(f"Error during API call for fk_header_submenu_tab_id {fk_header_submenu_tab_id}: {e}")
        except json.JSONDecodeError:
            print(f"Failed to decode JSON response from API for fk_header_submenu_tab_id {fk_header_submenu_tab_id}.")
        except Exception as e:
            print(f"An unexpected error occurred during scraping: {e}")
        finally:
            if conn:
                conn.close()
                print("Database connection closed.")

# --- Execute the scraper ---
if __name__ == "__main__":
    # Ensure aiofiles is installed: pip install aiofiles
    # Ensure aiohttp is installed: pip install aiohttp

    # Only try the specific ID 46 as requested
    asyncio.run(scrape_mufap_reports(46)) 

    print("\n--- Scraper run complete ---")
