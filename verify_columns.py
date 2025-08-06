import os
import pandas as pd
from collections import Counter

# --- Configuration ---
# Define the base directory where your report folders are located
BASE_DATA_DIR = "." # Current directory, adjust if your folders are elsewhere

REPORT_FOLDERS = {
    "PKISRV": os.path.join(BASE_DATA_DIR, "PKISRV"),
    "PKRV": os.path.join(BASE_DATA_DIR, "PKRV"),
    "PKFRV": os.path.join(BASE_DATA_DIR, "PKFRV")
}

# --- Helper Functions ---
def get_csv_header(filepath: str) -> list[str] | None:
    """
    Reads the header (first row) of a CSV file and returns it as a list of column names.
    Uses pandas to robustly read CSVs, handling various delimiters and quoting.
    Tries multiple encodings and skiprows values to find the actual header.
    """
    encodings_to_try = ['utf-8', 'latin1', 'cp1252']
    
    for skip_rows in range(5): # Try skipping 0 to 4 rows
        for encoding in encodings_to_try:
            try:
                # Attempt to read, assuming the first non-empty row after skipping is the header
                df = pd.read_csv(filepath, encoding=encoding, skiprows=skip_rows, nrows=0)
                
                # Check if the header looks like actual column names (not just a single long string)
                # If the first column name is a long string or looks like metadata, try skipping more
                if df.empty or len(df.columns) < 1: # Must have at least one column
                    continue 
                
                # Drop columns that are entirely unnamed (e.g., 'Unnamed: 0', 'Unnamed: 1')
                # These often result from irregular CSV formatting
                # Note: This is applied to the *header detection logic*, not the actual data load.
                # It helps in standardizing the detected header for comparison.
                cleaned_columns = [col for col in df.columns if not str(col).strip().lower().startswith('unnamed:')]
                
                # If after cleaning, there are very few columns or they still look like metadata,
                # it might mean the header is still not correctly identified.
                # This is a heuristic and might need fine-tuning for specific files.
                if not cleaned_columns: # If all columns were 'Unnamed' or empty, this might not be the real header
                    continue
                
                return cleaned_columns # Return the cleaned list of column names
            except pd.errors.EmptyDataError:
                # File is empty or no data after skipping rows
                continue
            except UnicodeDecodeError:
                # Encoding failed, try next encoding
                continue
            except Exception as e:
                # Other pandas reading errors, print and try next encoding/skip_rows
                # print(f"Debug: Error reading {filepath} with encoding {encoding} and skiprows {skip_rows}: {e}")
                continue
    
    # print(f"Warning: Could not read header from {filepath} with any of the tried encodings/skiprows.")
    return None

# --- Main Analysis Logic ---
def analyze_csv_columns():
    """
    Analyzes column structures of CSV files within each report folder,
    identifying the most common and all unique combinations.
    """
    print("--- Starting CSV Column Structure Analysis ---")

    for report_type, folder_path in REPORT_FOLDERS.items():
        print(f"\nAnalyzing folder: {folder_path} ({report_type} reports)")

        if not os.path.exists(folder_path):
            print(f"Folder does not exist: {folder_path}. Skipping.")
            continue
        if not os.path.isdir(folder_path):
            print(f"Path is not a directory: {folder_path}. Skipping.")
            continue

        csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

        if not csv_files:
            print(f"No CSV files found in {folder_path}.")
            continue

        # Use Counter to count occurrences of each unique header structure
        # Convert list of columns to a tuple so it's hashable for Counter
        header_counts = Counter()
        file_to_header_map = {} # To store which files map to which header structure

        for filename in csv_files:
            filepath = os.path.join(folder_path, filename)
            header = get_csv_header(filepath)
            if header is not None:
                header_tuple = tuple(header) # Convert list to tuple for hashing
                header_counts[header_tuple] += 1
                if header_tuple not in file_to_header_map:
                    file_to_header_map[header_tuple] = []
                file_to_header_map[header_tuple].append(filename)
            else:
                print(f"Could not determine header for file: {filename}. Skipping from analysis.")

        if not header_counts:
            print(f"No valid headers extracted from any CSV in {folder_path}.")
            continue

        print(f"\n--- Column Structure Summary for {report_type} ---")
        
        # Find the most common header structure
        most_common_header, most_common_count = header_counts.most_common(1)[0]
        print(f"Most Common Structure ({most_common_count} files):")
        print(f"  Columns: {list(most_common_header)}")
        print(f"  Example files: {file_to_header_map.get(most_common_header, [])[:3]}...") # Show first 3 examples

        print("\nAll Unique Column Structures and Their Counts:")
        for header_tuple, count in header_counts.most_common(): # Iterate in descending order of frequency
            print(f"  Count: {count}")
            print(f"  Columns: {list(header_tuple)}")
            # Optionally, list some files for each structure
            # print(f"  Files: {file_to_header_map.get(header_tuple, [])[:2]}...") # Show first 2 examples
            print("-" * 30)

    print("\n--- CSV Column Structure Analysis Complete ---")

# --- Execute the script ---
if __name__ == "__main__":
    # Ensure pandas is installed: pip install pandas
    analyze_csv_columns()
