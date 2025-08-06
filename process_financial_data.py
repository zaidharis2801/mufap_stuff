import pandas as pd
import sqlite3
import os
import glob
from pathlib import Path

# --- Configuration ---
# Databases
METADATA_DB_PATH = "mufap_data.db"
FINANCIAL_DB_PATH = "financial_data.db"

# Directories to scan for data files
DIRECTORIES_TO_SCAN = ["PKFRV", "PKRV"]
MAX_HEADER_SCAN_ROWS = 15

# --- Format 1: Mutual Fund Contribution (PKFRV) Configuration ---
PKFRV_CORE_COLUMNS = {
   'Issue Date', 'Maturity date', 'Coupon Frequency'
}

# --- Format 2: Tenor Rate (PKRV) Configuration ---
PKRV_EXACT_COLUMNS = {'Tenor', 'Mid Rate', 'Change'}


def load_metadata_cache(db_path):
    """
    Loads report metadata (date, title) from the mufap_data.db into a cache.
    The cache is a dictionary mapping a filename to its metadata.
    """
    print(f"--- Loading metadata from '{db_path}' ---")
    if not os.path.exists(db_path):
        print(f"[Warning] Metadata database not found at '{db_path}'. Dates will not be added.")
        return {}

    cache = {}
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT filepath, date, title FROM mufap_reports")
        for row in cursor.fetchall():
            filepath, date, title = row
            if filepath:
                # Use the filename as the key for easy lookup
                filename = os.path.basename(filepath)
                cache[filename] = {'date': date, 'title': title}
        conn.close()
        print(f"Successfully loaded metadata for {len(cache)} reports.")
    except Exception as e:
        print(f"[Error] Could not load metadata from '{db_path}': {e}")
    return cache

def setup_database(db_path):
    """
    Sets up the new financial_data.db.
    Deletes old db and creates the tenor_rates table with new metadata columns.
    """
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"\nRemoved existing database '{db_path}'.")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create the tenor_rates table with added metadata columns
    cursor.execute('''
        CREATE TABLE tenor_rates (
            unique_id INTEGER PRIMARY KEY AUTOINCREMENT,
            Tenor TEXT,
            "Mid Rate" REAL,
            Change REAL,
            report_date TEXT,
            source_filepath TEXT
        )
    ''')
    print("Table 'tenor_rates' created successfully.")
    conn.commit()
    return conn

def find_header_and_type(filepath):
    """
    Scans the first few rows of a file to find a header and determine its format.
    Standardizes core/exact columns to Title Case for matching.
    """
    file_extension = Path(filepath).suffix.lower()
    df_preview = None
    try:
        if file_extension == '.csv':
            df_preview = pd.read_csv(filepath, header=None, nrows=MAX_HEADER_SCAN_ROWS, sep=None, engine='python')
        elif file_extension in ['.xlsx', '.xls']:
            df_preview = pd.read_excel(filepath, header=None, nrows=MAX_HEADER_SCAN_ROWS, engine='openpyxl')
        else:
            return None, None
    except Exception as e:
        print(f"  [Error] Could not read preview from {Path(filepath).name}: {e}")
        return None, None

    # Standardize the required column sets to Title Case for reliable matching
    pkfrv_core_titlecase = {col.title() for col in PKFRV_CORE_COLUMNS}
    pkrv_exact_titlecase = {col.title() for col in PKRV_EXACT_COLUMNS}

    for i, row in df_preview.iterrows():
        # Clean potential header and convert to Title Case
        potential_header_list = [str(col).strip() for col in row.dropna()]
        potential_header_set = {col.title() for col in potential_header_list}

        if potential_header_set == pkrv_exact_titlecase:
            return i, 'PKRV'
        if pkfrv_core_titlecase.issubset(potential_header_set):
            return i, 'PKFRV'

    return None, None

def read_data_with_header(filepath, header_index):
    """Reads the full file using the identified header row index."""
    file_extension = Path(filepath).suffix.lower()
    try:
        if file_extension == '.csv':
            return pd.read_csv(filepath, skiprows=header_index, sep=None, engine='python')
        elif file_extension in ['.xlsx', '.xls']:
            return pd.read_excel(filepath, skiprows=header_index, engine='openpyxl')
    except Exception as e:
        print(f"  [Error] Could not read full data from {Path(filepath).name}: {e}")
        return None

def main():
    """Main function to orchestrate the file processing and database loading."""
    # Load metadata from the old database first
    metadata_cache = load_metadata_cache(METADATA_DB_PATH)

    # Setup the new database for financial data
    conn = setup_database(FINANCIAL_DB_PATH)

    # --- Phase 1: Scan all files, categorize, and stage for loading ---
    pkfrv_data_to_load = []
    all_pkfrv_columns = set()
    column_frequency = {}

    all_files = []
    for directory in DIRECTORIES_TO_SCAN:
        if os.path.isdir(directory):
            all_files.extend(glob.glob(os.path.join(directory, '*.csv')))
            all_files.extend(glob.glob(os.path.join(directory, '*.xlsx')))
        else:
            print(f"\n[Warning] Directory '{directory}' not found. Skipping.")

    if not all_files:
        print(f"\n[Warning] No .csv or .xlsx files found in specified directories.")
        conn.close()
        return

    print(f"\n--- Analyzing {len(all_files)} files ---")

    for filepath in all_files:
        filename = os.path.basename(filepath)
        print(f"\nProcessing file: {filename}")

        header_index, file_type = find_header_and_type(filepath)
        metadata = metadata_cache.get(filename, {'date': 'N/A', 'title': 'N/A'})

        if file_type == 'PKRV':
            if Path(filepath).suffix.lower() != '.csv':
                print(f"  -> Skipping: Tenor Rate (PKRV) format must be a .csv file.")
                continue

            print(f"  -> Identified as: Tenor Rate File (PKRV). Header on row {header_index + 1}.")
            df = read_data_with_header(filepath, header_index)
            if df is not None:
                # *** FIX: Standardize column names to prevent loading errors ***
                df.columns = [str(c).strip().title() for c in df.columns]
                
                # *** NEW: Add metadata columns ***
                df['report_date'] = metadata['date']
                df['source_filepath'] = filepath

                try:
                    df_to_load = df[['Tenor', 'Mid Rate', 'Change', 'report_date', 'source_filepath']]
                    df_to_load.to_sql('tenor_rates', conn, if_exists='append', index=False)
                    print(f"  -> Successfully loaded {len(df)} rows into 'tenor_rates'.")
                except Exception as e:
                    print(f"  [Error] Failed to load data into 'tenor_rates': {e}")

        elif file_type == 'PKFRV':
            print(f"  -> Identified as: Mutual Fund Contribution File (PKFRV). Header on row {header_index + 1}.")
            df = read_data_with_header(filepath, header_index)
            if df is not None:
                # *** FIX & NEW: Standardize columns and add metadata ***
                df.columns = [str(c).strip().title() for c in df.columns]
                df['Report_Date'] = metadata['date']
                df['Source_Filepath'] = filepath

                pkfrv_data_to_load.append(df)
                current_columns = set(df.columns)
                all_pkfrv_columns.update(current_columns)

                for col in current_columns:
                    column_frequency[col] = column_frequency.get(col, 0) + 1

                print(f"  -> Staged {len(df)} rows for batch loading into 'mutual_fund_data'.")
        else:
            print(f"  -> Skipping: Does not match any known format.")

    # --- Phase 2: Batch Load Mutual Fund Data ---
    if pkfrv_data_to_load:
        print("\n--- Batch Loading Mutual Fund Data ---")
        sorted_columns = sorted(list(all_pkfrv_columns))

        cursor = conn.cursor()
        cols_sql = ", ".join([f'"{col}"' for col in sorted_columns])
        create_table_sql = f"CREATE TABLE mutual_fund_data ({cols_sql})"
        
        # This will now succeed because of the standardized column names
        cursor.execute(create_table_sql)
        print(f"  -> Table 'mutual_fund_data' created with {len(sorted_columns)} unique columns.")

        total_rows_loaded = 0
        for i, df in enumerate(pkfrv_data_to_load, 1):
            for col in sorted_columns:
                if col not in df.columns:
                    df[col] = 0 # Insert 0 for missing data
            df_to_load = df[sorted_columns]
            df_to_load.to_sql('mutual_fund_data', conn, if_exists='append', index=False)
            total_rows_loaded += len(df_to_load)
        print(f"  -> Successfully loaded a total of {total_rows_loaded} rows.")

    # --- Phase 3: Column Frequency Report ---
    if column_frequency:
        print("\n--- Mutual Fund Data Column Frequency Report ---")
        sorted_report = sorted(column_frequency.items(), key=lambda item: item[0])
        for col, count in sorted_report:
            print(f"  - Column '{col}': Appeared in {count} file(s)")

    conn.close()
    print(f"\n--- Script Complete. Database saved to '{FINANCIAL_DB_PATH}' ---")

if __name__ == "__main__":
    main()