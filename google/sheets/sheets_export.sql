/*
===============================================================================
GOOGLE SHEETS EXPORT FOR SNOWFLAKE INTELLIGENCE
===============================================================================

This SQL script creates a complete Snowflake stored procedure that exports 
analysis results from Snowflake Intelligence to Google Sheets. The procedure
accepts JSON data and creates formatted tabs in a shared Google Spreadsheet.

WORKFLOW OVERVIEW:
1. Setup Google Cloud service account and enable Sheets API
2. Create Google Spreadsheet and share with service account  
3. Configure Snowflake secrets, network rules, and external access
4. Deploy the EXPORT_TO_GOOGLE_SHEETS stored procedure
5. Call procedure with JSON data to create formatted spreadsheet tabs

FEATURES:
- Accepts dynamic JSON data from Snowflake Intelligence workflows
- Creates timestamped tabs for organized data history
- Automatically detects headers from JSON objects
- Applies professional formatting (blue headers, auto-resize columns)
- Returns detailed success/error responses as JSON

PREREQUISITES:
- Google Cloud Project with Sheets API enabled
- Service Account with JSON key file
- Google Spreadsheet shared with service account (Editor permissions)
- Snowflake ACCOUNTADMIN role for external access setup

TABLE OF CONTENTS (Search for these markers):
- [SECTION_1_SCHEMAS]     : Database and schema creation
- [SECTION_2_SECRETS]     : Service account secret configuration  
- [SECTION_3_NETWORK]     : Network rules for Google APIs
- [SECTION_4_ACCESS]      : External access integration
- [SECTION_5_PROCEDURE]   : Main stored procedure definition
- [SECTION_6_EXAMPLES]    : Usage examples and test calls

===============================================================================
*/

-- [SECTION_1_SCHEMAS] Database and Schema Creation
-- ================================================
CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.INTEGRATIONS;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.TOOLS;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.AGENTS;

USE SCHEMA SNOWFLAKE_INTELLIGENCE.INTEGRATIONS;

-- [SECTION_2_SECRETS] Service Account Secret Configuration
-- =========================================================
-- Create secret for service account credentials
-- IMPORTANT: Replace this template with your actual Google Cloud service account JSON
CREATE OR REPLACE SECRET GOOGLE_SHEETS_SERVICE_ACCOUNT
TYPE = GENERIC_STRING
SECRET_STRING = '{
  "type": "service_account",
  "project_id": "YOUR_PROJECT_ID",
  "private_key_id": "YOUR_PRIVATE_KEY_ID",
  "private_key": "-----BEGIN PRIVATE KEY-----\\nYOUR_PRIVATE_KEY_CONTENT\\n-----END PRIVATE KEY-----\\n",
  "client_email": "YOUR_SERVICE_ACCOUNT@YOUR_PROJECT_ID.iam.gserviceaccount.com",
  "client_id": "YOUR_CLIENT_ID",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/YOUR_SERVICE_ACCOUNT%40YOUR_PROJECT_ID.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}';

SHOW SECRETS IN SCHEMA SNOWFLAKE_INTELLIGENCE.INTEGRATIONS;

-- [SECTION_3_NETWORK] Network Rules for Google APIs
-- ==================================================
-- Create network rule for Google APIs
CREATE OR REPLACE NETWORK RULE GOOGLE_APIS_NETWORK_RULE
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('sheets.googleapis.com:443', 'oauth2.googleapis.com:443', 'accounts.google.com:443');
SHOW NETWORK RULES IN SCHEMA SNOWFLAKE_INTELLIGENCE.INTEGRATIONS;

-- [SECTION_4_ACCESS] External Access Integration
-- ===============================================
USE ROLE ACCOUNTADMIN;

-- Create external access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GOOGLE_SHEETS_INTEGRATION
ALLOWED_NETWORK_RULES = (google_apis_network_rule)
ALLOWED_AUTHENTICATION_SECRETS = (SNOWFLAKE_INTELLIGENCE.INTEGRATIONS.GOOGLE_SHEETS_SERVICE_ACCOUNT)
ENABLED = TRUE;

-- [SECTION_5_PROCEDURE] Main Stored Procedure Definition
-- =======================================================
USE ROLE SYSADMIN;
USE SCHEMA SNOWFLAKE_INTELLIGENCE.TOOLS;

-- Create the stored procedure
CREATE OR REPLACE PROCEDURE EXPORT_TO_GOOGLE_SHEETS(
    data_json STRING,
    sheet_description STRING DEFAULT 'Analysis Results'
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = (
    'snowflake-snowpark-python',
    'google-api-python-client',
    'google-auth-httplib2',
    'google-auth-oauthlib'
)
EXTERNAL_ACCESS_INTEGRATIONS = (google_sheets_integration)
SECRETS = ('service_account_key' = SNOWFLAKE_INTELLIGENCE.INTEGRATIONS.GOOGLE_SHEETS_SERVICE_ACCOUNT)
HANDLER = 'main'
AS $$
import json
import logging
from datetime import datetime
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def authenticate_google_sheets(service_account_json: str) -> Any:
    """
    Authenticate with Google Sheets API using service account credentials.
    
    Args:
        service_account_json: JSON string containing service account credentials
        
    Returns:
        Google Sheets API service object
    """
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    
    try:
        # Parse the JSON credentials
        credentials_data = json.loads(service_account_json)
        
        # Create credentials from the parsed data
        credentials = service_account.Credentials.from_service_account_info(
            credentials_data, scopes=SCOPES
        )
        
        # Build the service with cache discovery disabled to avoid Snowflake logging issues
        service = build('sheets', 'v4', credentials=credentials, cache_discovery=False)
        return service
        
    except Exception as e:
        raise Exception(f"Authentication failed: {str(e)}")

def add_worksheet(service: Any, spreadsheet_id: str, sheet_name: str) -> int:
    """
    Add a new worksheet to an existing spreadsheet.
    
    Args:
        service: Google Sheets API service object
        spreadsheet_id: ID of the spreadsheet
        sheet_name: Name for the new worksheet
        
    Returns:
        Sheet ID of the created worksheet
    """
    request_body = {
        'requests': [{
            'addSheet': {
                'properties': {
                    'title': sheet_name,
                    'gridProperties': {
                        'rowCount': 100,
                        'columnCount': 10
                    }
                }
            }
        }]
    }
    
    try:
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=request_body
        ).execute()
        
        sheet_id = response['replies'][0]['addSheet']['properties']['sheetId']
        return sheet_id
        
    except HttpError as e:
        raise Exception(f"Error adding worksheet: {str(e)}")

def insert_dynamic_data(service: Any, spreadsheet_id: str, sheet_name: str, data_json: str) -> int:
    """
    Insert dynamic data from JSON into a worksheet.
    
    Args:
        service: Google Sheets API service object
        spreadsheet_id: ID of the spreadsheet
        sheet_name: Name of the worksheet
        data_json: JSON string containing array of data rows
        
    Returns:
        Number of cells updated
    """
    try:
        # Parse the JSON data
        data_array = json.loads(data_json)
        
        # Ensure we have data
        if not data_array or len(data_array) == 0:
            raise ValueError("No data provided")
        
        # Convert data to list of lists for Google Sheets
        if isinstance(data_array[0], dict):
            # If data is array of objects, extract headers and values
            headers = list(data_array[0].keys())
            sheet_data = [headers]  # First row is headers
            
            for row in data_array:
                sheet_data.append([str(row.get(col, '')) for col in headers])
        else:
            # If data is already array of arrays, use as-is
            sheet_data = data_array
        
        # Calculate range based on actual data size
        num_rows = len(sheet_data)
        num_cols = len(sheet_data[0]) if sheet_data else 0
        
        # Convert column number to letter (A, B, C, etc.)
        def col_to_letter(col_num):
            result = ""
            while col_num > 0:
                col_num -= 1
                result = chr(col_num % 26 + ord('A')) + result
                col_num //= 26
            return result
        
        end_col = col_to_letter(num_cols)
        range_name = f'{sheet_name}!A1:{end_col}{num_rows}'
        
        body = {
            'values': sheet_data
        }
        
    except (json.JSONDecodeError, ValueError, KeyError) as e:
        raise ValueError(f"Invalid data format: {str(e)}")
    
    try:
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()
        
        return result.get('updatedCells', 0)
        
    except HttpError as e:
        raise Exception(f"Error inserting data: {str(e)}")

def format_header_row(service: Any, spreadsheet_id: str, sheet_id: int) -> None:
    """
    Format the header row with bold text and background color.
    
    Args:
        service: Google Sheets API service object
        spreadsheet_id: ID of the spreadsheet
        sheet_id: ID of the worksheet
    """
    requests = [
        {
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': {
                            'red': 0.2,
                            'green': 0.5,
                            'blue': 0.8
                        },
                        'textFormat': {
                            'bold': True,
                            'foregroundColor': {
                                'red': 1.0,
                                'green': 1.0,
                                'blue': 1.0
                            }
                        }
                    }
                },
                'fields': 'userEnteredFormat(backgroundColor,textFormat)'
            }
        },
        {
            'autoResizeDimensions': {
                'dimensions': {
                    'sheetId': sheet_id,
                    'dimension': 'COLUMNS',
                    'startIndex': 0,
                    'endIndex': 7
                }
            }
        }
    ]
    
    body = {'requests': requests}
    
    try:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()
        
    except HttpError as e:
        raise Exception(f"Error formatting header: {str(e)}")

def main(session, data_json: str, sheet_description: str) -> str:
    """
    Main handler function for the stored procedure.
    
    Args:
        session: Snowflake session object
        data_json: JSON string containing the data to export
        sheet_description: Description for the data being exported
        
    Returns:
        Success message with details
    """
    try:
        # IMPORTANT: Replace with your Google Spreadsheet ID
        # Get this from your spreadsheet URL: https://docs.google.com/spreadsheets/d/YOUR_SPREADSHEET_ID/edit
        spreadsheet_id = 'YOUR_SPREADSHEET_ID'
        
        # Get the service account credentials from the secret
        import _snowflake
        service_account_json = _snowflake.get_generic_secret_string('service_account_key')
        
        # Authenticate with Google Sheets API
        sheets_service = authenticate_google_sheets(service_account_json)
        
        # Create timestamp for this run
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create sheet name based on description and timestamp
        sheet_name = f"{sheet_description}_{timestamp}"
        
        # Add a new worksheet tab for this execution
        sheet_id = add_worksheet(sheets_service, spreadsheet_id, sheet_name)
        
        # Insert the dynamic data into the new tab
        updated_cells = insert_dynamic_data(sheets_service, spreadsheet_id, sheet_name, data_json)
        
        # Format the header row
        format_header_row(sheets_service, spreadsheet_id, sheet_id)
        
        # Build success message
        success_message = {
            "status": "success",
            "spreadsheet_id": spreadsheet_id,
            "sheet_name": sheet_name,
            "updated_cells": updated_cells,
            "timestamp": timestamp,
            "url": f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        }
        
        return json.dumps(success_message)
        
    except Exception as e:
        # Build error message with more details
        import traceback
        error_details = traceback.format_exc()
        error_message = {
            "status": "error",
            "error": str(e),
            "details": error_details.replace('\n', ' | ').replace('\r', ' '),
            "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S")
        }
        
        return json.dumps(error_message)
$$;

-- [SECTION_6_EXAMPLES] Usage Examples and Test Calls
-- ====================================================

-- Example 1: Sales Analysis Data
CALL export_to_google_sheets(
    '[
      {"date": "2025-01-15", "product": "Widget A", "quantity": 25, "revenue": 749.75, "region": "North America"},
      {"date": "2025-01-15", "product": "Widget B", "quantity": 18, "revenue": 899.82, "region": "Europe"},
      {"date": "2025-01-15", "product": "Widget C", "quantity": 32, "revenue": 1279.68, "region": "Asia Pacific"},
      {"date": "2025-01-16", "product": "Widget A", "quantity": 19, "revenue": 569.81, "region": "North America"},
      {"date": "2025-01-16", "product": "Widget B", "quantity": 23, "revenue": 1149.77, "region": "Europe"}
    ]',
    'Sales Analysis'
  );

-- Example 2: Customer Performance Data  
CALL export_to_google_sheets(
    '[
      {"customer_id": 1001, "customer_name": "Acme Corp", "total_orders": 45, "total_revenue": 125000, "avg_order_value": 2777.78},
      {"customer_id": 1002, "customer_name": "TechFlow Solutions", "total_orders": 32, "total_revenue": 89500, "avg_order_value": 2796.88},
      {"customer_id": 1003, "customer_name": "Global Industries", "total_orders": 67, "total_revenue": 198750, "avg_order_value": 2966.42}
    ]',
    'Customer Performance'
  );

-- Example 3: Financial Metrics
CALL export_to_google_sheets(
    '[
      {"metric": "Total Revenue", "q1_2025": 2500000, "q4_2024": 2350000, "growth_rate": 6.38},
      {"metric": "Active Customers", "q1_2025": 1250, "q4_2024": 1180, "growth_rate": 5.93},
      {"metric": "Average Order Value", "q1_2025": 2000, "q4_2024": 1992, "growth_rate": 0.40}
    ]',
    'Quarterly Metrics'
  );