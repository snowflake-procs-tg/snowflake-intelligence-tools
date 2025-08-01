/*
===============================================================================
GOOGLE DOCS EXPORT FOR SNOWFLAKE INTELLIGENCE
===============================================================================

This SQL script creates a complete Snowflake stored procedure that exports 
analysis results from Snowflake Intelligence to Google Docs. The procedure
accepts markdown content and creates formatted documents with professional styling.

WORKFLOW OVERVIEW:
1. Setup Google Cloud service account and enable Docs API
2. Configure Snowflake secrets, network rules, and external access
3. Deploy the EXPORT_TO_GOOGLE_DOCS stored procedure
4. Call procedure with markdown content to create formatted documents

FEATURES:
- Accepts dynamic markdown content from Snowflake Intelligence workflows
- Converts markdown formatting to Google Docs styling
- Handles headers, bold text, italics, lists, and paragraphs
- Creates professional documents with proper formatting
- Returns detailed success/error responses as JSON

PREREQUISITES:
- Google Cloud Project with Docs API enabled
- Service Account with JSON key file
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
CREATE OR REPLACE SECRET GOOGLE_DOCS_SERVICE_ACCOUNT
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
CREATE OR REPLACE NETWORK RULE GOOGLE_DOCS_NETWORK_RULE
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('docs.googleapis.com:443', 'oauth2.googleapis.com:443', 'accounts.google.com:443');

SHOW NETWORK RULES IN SCHEMA SNOWFLAKE_INTELLIGENCE.INTEGRATIONS;

-- [SECTION_4_ACCESS] External Access Integration
-- ===============================================
USE ROLE ACCOUNTADMIN;

-- Create external access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GOOGLE_DOCS_INTEGRATION
ALLOWED_NETWORK_RULES = (GOOGLE_DOCS_NETWORK_RULE)
ALLOWED_AUTHENTICATION_SECRETS = (SNOWFLAKE_INTELLIGENCE.INTEGRATIONS.GOOGLE_DOCS_SERVICE_ACCOUNT)
ENABLED = TRUE;

-- [SECTION_5_PROCEDURE] Main Stored Procedure Definition
-- =======================================================
USE ROLE SYSADMIN;
USE SCHEMA SNOWFLAKE_INTELLIGENCE.TOOLS;

-- Create the stored procedure
CREATE OR REPLACE PROCEDURE EXPORT_TO_GOOGLE_DOCS(
    markdown_content STRING,
    document_id STRING DEFAULT 'YOUR_GOOGLE_DOCUMENT_ID'  -- Replace with your shared document ID
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
EXTERNAL_ACCESS_INTEGRATIONS = (GOOGLE_DOCS_INTEGRATION)
SECRETS = ('service_account_key' = SNOWFLAKE_INTELLIGENCE.INTEGRATIONS.GOOGLE_DOCS_SERVICE_ACCOUNT)
HANDLER = 'main'
AS $$
import json
import logging
from datetime import datetime
from typing import Any, Dict, List

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def authenticate_google_docs(service_account_json: str) -> Any:
    """
    Authenticate with Google Docs API using service account credentials.
    
    Args:
        service_account_json: JSON string containing service account credentials
        
    Returns:
        Google Docs API service object
    """
    SCOPES = [
        'https://www.googleapis.com/auth/documents',
        'https://www.googleapis.com/auth/drive.file'
    ]
    
    try:
        # Parse the JSON credentials
        credentials_data = json.loads(service_account_json)
        
        # Create credentials from the parsed data
        credentials = service_account.Credentials.from_service_account_info(
            credentials_data, scopes=SCOPES
        )
        
        # Build the service with cache discovery disabled to avoid Snowflake logging issues
        service = build('docs', 'v1', credentials=credentials, cache_discovery=False)
        return service
        
    except Exception as e:
        raise Exception(f"Authentication failed: {str(e)}")

def get_document_length(service: Any, document_id: str) -> int:
    """
    Get the current length of the document to know where to append.
    
    Args:
        service: Google Docs API service object
        document_id: ID of the document
        
    Returns:
        The end index of the document
    """
    try:
        document = service.documents().get(documentId=document_id).execute()
        # The content ends at the last index
        return document['body']['content'][-1]['endIndex'] - 1
        
    except HttpError as e:
        raise Exception(f"Error getting document: {str(e)}")

def append_markdown_to_docs(service: Any, document_id: str, markdown_content: str) -> int:
    """
    Append markdown content to existing Google Doc with timestamp separator.
    
    Args:
        service: Google Docs API service object
        document_id: ID of the target document
        markdown_content: Markdown string from Snowflake Intelligence
        
    Returns:
        Number of requests executed
    """
    import re
    
    try:
        # Get current document length to know where to append
        doc_length = get_document_length(service, document_id)
        current_index = doc_length
        
        requests = []
        
        # Skip the timestamp separator - just add content directly
        # Add a small separator between entries if document already has content
        if current_index > 1:  # Document has existing content
            separator_text = "\\n\\n"
            requests.append({
                'insertText': {
                    'text': separator_text,
                    'location': {'index': current_index}
                }
            })
            current_index += len(separator_text)
        
        # Process markdown content - convert escaped newlines first
        clean_content = markdown_content.replace('\\\\n', '\n')
        
        # Process entire content at once for better formatting control
        format_requests = []
        
        # First, insert all the text with basic markdown-to-text conversion
        processed_content = clean_content
        
        # Process content to remove excessive spacing and track headers
        header_style_ranges = []
        lines = processed_content.split('\n')
        processed_lines = []
        
        for i, line in enumerate(lines):
            # Handle headers
            header_match = re.match(r'^(#{1,6})\s+(.+)$', line)
            if header_match:
                level = len(header_match.group(1))
                header_text = header_match.group(2)
                
                # Track header position for styling
                heading_types = {
                    1: 'HEADING_1', 2: 'HEADING_2', 3: 'HEADING_3',
                    4: 'HEADING_4', 5: 'HEADING_5', 6: 'HEADING_6'
                }
                
                # Calculate position in final content
                content_before_header = '\n'.join(processed_lines)
                start_pos = len(content_before_header) + (1 if content_before_header else 0)  # +1 for newline
                
                header_style_ranges.append({
                    'start': current_index + start_pos,
                    'end': current_index + start_pos + len(header_text),
                    'style': heading_types.get(level, 'NORMAL_TEXT')
                })
                
                processed_lines.append(header_text)
                
            # Handle bullet points  
            elif re.match(r'^[-*]\s+(.+)$', line):
                bullet_match = re.match(r'^[-*]\s+(.+)$', line)
                bullet_text = f"• {bullet_match.group(1)}"
                processed_lines.append(bullet_text)
                
                # Track bullet for font sizing
                content_before_bullet = '\n'.join(processed_lines[:-1])
                bullet_start = len(content_before_bullet) + (1 if content_before_bullet else 0)
                
                header_style_ranges.append({
                    'start': current_index + bullet_start,
                    'end': current_index + bullet_start + len(bullet_text),
                    'style': 'BULLET_FONT_SIZE'  # Special marker for bullet formatting
                })
                
            # Handle empty lines - reduce excessive spacing
            elif not line.strip():
                # Only add empty line if the previous line wasn't empty and wasn't a header
                if processed_lines and processed_lines[-1].strip():
                    # Check if previous line was a header (no # symbols in processed content)
                    prev_was_header = any(r['end'] == current_index + len('\n'.join(processed_lines)) for r in header_style_ranges)
                    if not prev_was_header:
                        processed_lines.append('')
                        
            # Handle regular lines
            else:
                processed_lines.append(line)
        
        # Rebuild content with controlled spacing
        processed_content = '\n'.join(processed_lines)
        
        # Insert the processed content
        requests.append({
            'insertText': {
                'text': processed_content,
                'location': {'index': current_index}
            }
        })
        
        # Apply header styles and bullet font sizing
        for style_range in header_style_ranges:
            if style_range['style'] == 'BULLET_FONT_SIZE':
                # Apply 12pt font size to bullets
                requests.append({
                    'updateTextStyle': {
                        'textStyle': {
                            'fontSize': {'magnitude': 12, 'unit': 'PT'}
                        },
                        'fields': 'fontSize',
                        'range': {
                            'startIndex': style_range['start'],
                            'endIndex': style_range['end']
                        }
                    }
                })
            else:
                # Apply header paragraph styles
                requests.append({
                    'updateParagraphStyle': {
                        'paragraphStyle': {
                            'namedStyleType': style_range['style']
                        },
                        'fields': 'namedStyleType',
                        'range': {
                            'startIndex': style_range['start'],
                            'endIndex': style_range['end'] + 1  # Include the newline for paragraph styling
                        }
                    }
                })
        
        # Find and apply bold formatting (**text**)
        bold_pattern = r'\*\*([^*]+?)\*\*'
        for match in re.finditer(bold_pattern, processed_content):
            start_pos = current_index + match.start(1)
            end_pos = current_index + match.end(1)
            requests.append({
                'updateTextStyle': {
                    'textStyle': {'bold': True},
                    'fields': 'bold',
                    'range': {
                        'startIndex': start_pos,
                        'endIndex': end_pos
                    }
                }
            })
        
        # Find and apply italic formatting (*text*) - avoid conflicts with bold
        # Process content without ** first to find single *
        content_without_bold = re.sub(r'\*\*[^*]+?\*\*', '', processed_content)
        italic_pattern = r'\*([^*\n]+?)\*'
        for match in re.finditer(italic_pattern, content_without_bold):
            # Find the actual position in the original content
            match_text = f"*{match.group(1)}*"
            actual_pos = processed_content.find(match_text)
            if actual_pos != -1:
                start_pos = current_index + actual_pos + 1  # +1 to skip the *
                end_pos = current_index + actual_pos + len(match_text) - 1  # -1 to skip the *
                requests.append({
                    'updateTextStyle': {
                        'textStyle': {'italic': True},
                        'fields': 'italic',
                        'range': {
                            'startIndex': start_pos,
                            'endIndex': end_pos
                        }
                    }
                })
        
        # Clean up markdown syntax - replace the entire content with clean version
        cleaned_content = processed_content
        
        # Remove bold markers (**text** -> text)
        cleaned_content = re.sub(r'\*\*([^*]+?)\*\*', r'\1', cleaned_content)
        
        # Remove italic markers (*text* -> text) - avoid conflicts with bold
        content_without_bold = re.sub(r'\*\*[^*]+?\*\*', lambda m: m.group(0).replace('*', '★'), cleaned_content)
        cleaned_content = re.sub(r'\*([^*\n]+?)\*', r'\1', content_without_bold)
        cleaned_content = cleaned_content.replace('★', '*')  # Restore any ** that were protected
        
        # Replace the entire content with cleaned version
        cleanup_requests = [{
            'deleteContentRange': {
                'range': {
                    'startIndex': current_index,
                    'endIndex': current_index + len(processed_content)
                }
            }
        }, {
            'insertText': {
                'text': cleaned_content,
                'location': {'index': current_index}
            }
        }]
        
        # Execute formatting requests first
        if requests:
            service.documents().batchUpdate(
                documentId=document_id,
                body={'requests': requests}
            ).execute()
        
        # Then cleanup markdown syntax
        if cleanup_requests:
            service.documents().batchUpdate(
                documentId=document_id,
                body={'requests': cleanup_requests}
            ).execute()
        
        return len(requests) + len(cleanup_requests)
        
    except Exception as e:
        raise Exception(f"Error processing markdown: {str(e)}")

def main(session, markdown_content: str, document_id: str) -> str:
    """
    Main handler function for the stored procedure.
    
    Args:
        session: Snowflake session object
        markdown_content: Markdown string from Snowflake Intelligence
        document_id: ID of the existing shared document
        
    Returns:
        Success message with details
    """
    try:
        # Get the service account credentials from the secret
        import _snowflake
        service_account_json = _snowflake.get_generic_secret_string('service_account_key')
        
        # Authenticate with Google Docs API
        docs_service = authenticate_google_docs(service_account_json)
        
        # Create timestamp for this run
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Append markdown content to the existing document
        requests_count = append_markdown_to_docs(docs_service, document_id, markdown_content)
        
        # Build success message
        success_message = {
            "status": "success",
            "document_id": document_id,
            "action": "appended",
            "requests_executed": requests_count,
            "timestamp": timestamp,
            "url": f"https://docs.google.com/document/d/{document_id}"
        }
        
        return json.dumps(success_message)
        
    except Exception as e:
        # Build error message with more details
        import traceback
        error_details = traceback.format_exc()
        error_message = {
            "status": "error",
            "error": str(e),
            "details": error_details.replace('\\n', ' | ').replace('\\r', ' '),
            "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S")
        }
        
        return json.dumps(error_message)
$$;

-- [SECTION_6_EXAMPLES] Usage Examples and Test Calls
-- ====================================================

-- Example 1: Append Sales Analysis to shared document (using default doc ID)
CALL EXPORT_TO_GOOGLE_DOCS(
'# Sales Performance Analysis

## Executive Summary

Our Q1 2025 sales performance shows **strong growth** across all key metrics, with total revenue reaching $2.5M and a **6.38% growth rate** compared to the previous quarter.

## Key Metrics

### Revenue Performance
- **Total Revenue**: $2,500,000 (↑ 6.38%)
- **Active Customers**: 1,250 (↑ 5.93%)  
- **Average Order Value**: $2,000 (↑ 0.40%)

### Regional Performance
1. **North America**: Leading region with 65% of total revenue
2. **Europe**: Growing market with 4.2 customer satisfaction score
3. **Asia Pacific**: Emerging market with 15.7% market share

## Recommendations

- *Continue investing* in North American market expansion
- *Focus on customer satisfaction* improvements in Europe
- *Develop targeted strategies* for Asia Pacific growth

## Conclusion

The data indicates a **positive trajectory** for our sales performance, with opportunities for strategic growth across all regions.'
);  -- Uses default document ID: YOUR_GOOGLE_DOCUMENT_ID

-- Example 2: Append to a different document by specifying document ID
CALL EXPORT_TO_GOOGLE_DOCS(
'# Customer Performance Report

## Overview

Analysis of our **top-performing customers** reveals strong engagement and revenue contribution patterns.

## Top Customers

### Enterprise Clients
- **Acme Corp**: 45 orders, $125,000 revenue (avg: $2,777.78)
- **Global Industries**: 67 orders, $198,750 revenue (avg: $2,966.42)

### Mid-Market Clients  
- **TechFlow Solutions**: 32 orders, $89,500 revenue (avg: $2,796.88)

## Key Insights

1. Enterprise clients show **higher order values**
2. *Volume-based customers* demonstrate strong loyalty
3. **Average order values** range from $2,777 to $2,966

## Action Items

- Implement *tiered pricing* for enterprise clients
- Develop **loyalty programs** for high-volume customers
- *Expand* mid-market customer acquisition'
);

-- Example 3: Financial Quarterly Review
CALL EXPORT_TO_GOOGLE_DOCS(
    '# Quarterly Financial Review - Q1 2025

## Financial Highlights

Strong performance across all **key financial indicators** with sustained growth momentum.

## Core Metrics

### Revenue Metrics
1. **Total Revenue**: $2,500,000
   - Growth Rate: **6.38%** YoY
   - Target Achievement: *104%*

2. **Customer Metrics**: 1,250 Active Customers  
   - Growth Rate: **5.93%** YoY
   - Retention Rate: *92%*

3. **Order Metrics**: $2,000 Average Order Value
   - Growth Rate: **0.40%** YoY
   - Consistency: *High*

## Strategic Outlook

The financial results demonstrate **sustainable growth** with strong fundamentals across:

- Revenue expansion
- Customer base growth  
- Order value stability

*Next quarter focus*: Accelerating growth initiatives while maintaining operational efficiency.',
    'Q1 2025 Financial Review'
);