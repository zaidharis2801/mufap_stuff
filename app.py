import pandas as pd
import sqlite3
import os
import numpy as np
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.templating import Jinja2Templates

# --- Configuration ---
DATABASE_PATH = "financial_data.db"
FILES_BASE_DIRECTORY = "." 

# --- FastAPI App Initialization ---
app = FastAPI(
    title="Interactive Financial Data Viewer",
    description="An interactive web interface to view, sort, filter, and download financial data.",
    version="5.0.0"
)

# Setup Jinja2 templates to load from the 'templates' directory
templates = Jinja2Templates(directory="templates")

# --- Helper Functions ---

def get_table_config(report_type: str):
    """Returns the table name and column configuration for a given report type."""
    if report_type.upper() == "PKFRV":
        return {
            "table_name": "mutual_fund_data",
            "fixed_order": ['Report_Date', 'Issue Date', 'Maturity Date', 'Coupon Frequency'],
            "display_name": "Mutual Fund Data"
        }
    elif report_type.upper() == "PKRV":
        return {
            "table_name": "tenor_rates",
            "fixed_order": ['report_date', 'Tenor', 'Mid Rate', 'Change'],
            "display_name": "Tenor Rates"
        }
    else:
        raise HTTPException(status_code=404, detail="Report type not found.")

def get_display_columns(conn: sqlite3.Connection, table_name: str, fixed_order: list):
    """Gets the final, ordered list of columns to be displayed in the UI."""
    # Use PRAGMA to get table column info, which is faster than a SELECT * LIMIT 1
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name});")
    all_db_cols = [row[1] for row in cursor.fetchall()]
    
    # Columns to exclude from display
    cols_to_exclude = {'unique_id', 'Source_Filepath', 'source_filepath', '﻿'}
    unnamed_cols = {col for col in all_db_cols if 'unnamed' in col.lower()}
    cols_to_exclude.update(unnamed_cols)
    data_cols = [col for col in all_db_cols if col not in cols_to_exclude]

    final_ordered_cols = []
    # Create case-insensitive mapping of database columns
    db_cols_lower = {c.lower(): c for c in all_db_cols}
    
    # Add fixed-order columns (case-insensitive matching)
    for col in fixed_order:
        if col.lower() in db_cols_lower:
            actual_col = db_cols_lower[col.lower()]
            # Only add if it's not in the exclusion list
            if actual_col not in cols_to_exclude:
                final_ordered_cols.append(actual_col)
    
    # Add remaining columns (sorted alphabetically)
    remaining_cols = sorted([col for col in data_cols if col not in final_ordered_cols])
    final_ordered_cols.extend(remaining_cols)
    return final_ordered_cols

def convert_to_json_serializable(obj):
    """Convert numpy/pandas types to Python native types for JSON serialization."""
    if isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, (pd.Timestamp, pd.DatetimeTZDtype)):
        return obj.isoformat()
    elif pd.isna(obj):
        return None
    return obj

# --- API Endpoints ---

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Serves the main landing page without any data."""
    return templates.TemplateResponse("viewer.html", {"request": request, "data": None})

@app.get("/data/{report_type}", response_class=HTMLResponse)
async def get_report_page(request: Request, report_type: str):
    """
    Serves the main HTML page structure for a report type.
    The data will be fetched via a separate API call from the browser.
    """
    config = get_table_config(report_type)
    conn = sqlite3.connect(DATABASE_PATH)
    try:
        headers = get_display_columns(conn, config['table_name'], config['fixed_order'])
    finally:
        conn.close()

    return templates.TemplateResponse("viewer.html", {
        "request": request,
        "table_name": config['display_name'],
        "report_type_raw": report_type.upper(),
        "headers": headers
    })

@app.post("/api/data/{report_type}")
async def get_report_data_api(request: Request, report_type: str):
    """
    API endpoint for DataTables server-side processing.
    Handles pagination, searching, and sorting using direct SQL queries for efficiency.
    """
    try:
        form_data = await request.form()
        draw = int(form_data.get("draw", 1))
        start = int(form_data.get("start", 0))
        length = int(form_data.get("length", 100))
        search_value = form_data.get("search[value]", "").strip()
        
        order_column_index = int(form_data.get("order[0][column]", 0))
        order_dir = form_data.get("order[0][dir]", "asc")

        config = get_table_config(report_type)
        table_name = config['table_name']
        
        conn = sqlite3.connect(DATABASE_PATH)
        try:
            # Get the ordered list of columns that are actually displayed
            display_cols = get_display_columns(conn, table_name, config['fixed_order'])
            
            # Determine sort column safely - handle out of bounds
            if order_column_index < len(display_cols):
                sort_column = display_cols[order_column_index]
            else:
                # Default to first column if index is out of bounds
                sort_column = display_cols[0] if display_cols else None
            
            if not sort_column:
                raise ValueError("No valid columns found to sort by")
            
            # Get actual column names from database to find source filepath column
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            all_db_cols = [row[1] for row in cursor.fetchall()]
            
            # Find the source filepath column (case-insensitive)
            source_filepath_col = None
            for col in all_db_cols:
                if col.lower() in ['source_filepath', 'source filepath']:
                    source_filepath_col = col
                    break
            
            if not source_filepath_col:
                raise ValueError(f"Source filepath column not found in table {table_name}")
            
            cols_to_select = ", ".join([f'"{c}"' for c in display_cols] + [f'"{source_filepath_col}" AS download_path'])
            query = f"SELECT {cols_to_select} FROM {table_name}"
            
            # Total records
            total_records_df = pd.read_sql_query(f"SELECT COUNT(*) FROM {table_name}", conn)
            records_total = int(total_records_df.iloc[0, 0])

            # Filtering (Search)
            params = []
            if search_value:
                search_clauses = [f'CAST("{col}" AS TEXT) LIKE ?' for col in display_cols]
                query += f" WHERE {' OR '.join(search_clauses)}"
                params.extend([f'%{search_value}%'] * len(display_cols))
            
            # Filtered records count
            if search_value:
                count_query = f"SELECT COUNT(*) FROM {table_name}"
                if search_value:
                    count_query += f" WHERE {' OR '.join(search_clauses)}"
                records_filtered_df = pd.read_sql_query(count_query, conn, params=params)
                records_filtered = int(records_filtered_df.iloc[0, 0])
            else:
                records_filtered = records_total
            
            # Sorting and Pagination
            query += f' ORDER BY "{sort_column}" {order_dir.upper()}'
            query += " LIMIT ? OFFSET ?"
            params.extend([length, start])
            
            # Fetch the page of data
            df_page = pd.read_sql_query(query, conn, params=params)

        finally:
            conn.close()

        # Perform final transformations on the small, fetched page
        for col in df_page.columns:
            if 'date' in col.lower():
                # Suppress warning by specifying format or using faster method
                df_page[col] = pd.to_datetime(df_page[col], errors='coerce', format='mixed').dt.strftime('%Y-%m-%d').fillna('')
        
        df_page['display_filename'] = df_page['download_path'].apply(os.path.basename)
        
        # Replace NaN values with None
        df_page = df_page.replace({np.nan: None})
        
        # Convert to dictionary and ensure all numpy types are converted to native Python types
        data_dicts = df_page.to_dict(orient="records")
        
        # Convert all numpy types to JSON-serializable types
        data_dicts = [
            {key: convert_to_json_serializable(value) for key, value in record.items()}
            for record in data_dicts
        ]

        return JSONResponse({
            "draw": draw,
            "recordsTotal": records_total,
            "recordsFiltered": records_filtered,
            "data": data_dicts,
        })
    
    except Exception as e:
        # Log the error and return a proper error response
        print(f"Error in get_report_data_api: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/download/{filepath:path}")
async def download_file(filepath: str):
    """Provides a file for download."""
    full_path = os.path.join(FILES_BASE_DIRECTORY, filepath)
    if not os.path.exists(full_path):
        raise HTTPException(status_code=404, detail="File not found.")
    return FileResponse(path=full_path, filename=os.path.basename(full_path))