# Snowflake Intelligence Tools - Demonstration Examples

A collection of demonstration tools showcasing how to extend Snowflake Intelligence with external service integrations. These examples illustrate patterns for communication and data export capabilities.

**Note**: This code is for demonstration purposes only and should be adapted for your specific use cases.

## 📋 Table of Contents

- [Overview](#overview)
- [Email Tools](#email-tools)
  - [Email Sender](#email-sender)
- [Google Docs Integration](#google-docs-integration)
  - [Docs Export](#docs-export)
- [Google Sheets Integration](#google-sheets-integration)
  - [Sheets Export](#sheets-export)
- [Quick Start](#quick-start)
- [Prerequisites](#prerequisites)
- [Troubleshooting](#troubleshooting)

## 🎯 Overview

This repository contains Snowflake stored procedures that extend Snowflake Intelligence with:

- **Email Notifications**: Send emails directly from Snowflake using native notification integrations
- **Google Docs Export**: Convert markdown content to formatted Google Docs
- **Google Sheets Export**: Export JSON data to formatted Google Sheets with timestamped tabs

## 📧 Email Tools

### Email Sender
**File**: `email/email_sender.sql`

Send emails directly from Snowflake Intelligence using Snowflake's native notification integration.

**Features**:
- Direct email sending with immediate response
- Pre-approved recipient validation
- Plain text email support
- JSON response with success/failure details

**Usage**:
```sql
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.SEND_EMAIL(
    'recipient@example.com',
    'Subject Line',
    'Email body content'
);
```

## 📄 Google Docs Integration

### Docs Export
**File**: `google/docs/docs_export.sql`

Export markdown-formatted content from Snowflake Intelligence to Google Docs with automatic formatting.

**Features**:
- Markdown to Google Docs conversion
- Support for headers, bold, italic, lists
- Appends to existing documents
- Automatic formatting with document styles

**Usage**:
```sql
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.EXPORT_TO_GOOGLE_DOCS(
    '# Report Title

    ## Section 1
    This is **bold** and this is *italic*.
    
    - Bullet point 1
    - Bullet point 2',
    'YOUR_GOOGLE_DOC_ID'  -- Optional, uses default if not provided
);
```

## 📊 Google Sheets Integration

### Sheets Export
**File**: `google/sheets/sheets_export.sql`

Export JSON data from Snowflake Intelligence to Google Sheets with automatic formatting.

**Features**:
- Dynamic JSON data handling
- Automatic header detection
- Timestamped tabs for data history
- Example formatting (blue headers, auto-resize)

**Usage**:
```sql
-- Export array of objects
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.EXPORT_TO_GOOGLE_SHEETS(
    '[
      {"date": "2025-01-15", "product": "Widget A", "quantity": 25, "revenue": 749.75},
      {"date": "2025-01-16", "product": "Widget B", "quantity": 18, "revenue": 899.82}
    ]',
    'Sales Report'
);

-- Export array of arrays
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.EXPORT_TO_GOOGLE_SHEETS(
    '[
      ["Date", "Product", "Quantity", "Revenue"],
      ["2025-01-15", "Widget A", 25, 749.75],
      ["2025-01-16", "Widget B", 18, 899.82]
    ]',
    'Sales Data'
);
```

## 🚀 Quick Start

### 1. Email Setup
```sql
-- Run email/email_sender.sql first, then:
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.SEND_EMAIL(
    'your.email@company.com',
    'Test Email',
    'This is a test from Snowflake Intelligence!'
);
```

### 2. Google Docs Setup
```sql
-- 1. Create a Google Doc and share with service account
-- 2. Update GOOGLE_DOCS_SERVICE_ACCOUNT secret
-- 3. Run google/docs/docs_export.sql
-- 4. Test:
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.EXPORT_TO_GOOGLE_DOCS(
    '# Test Document\n\nThis is a **test** export.',
    'YOUR_DOC_ID'
);
```

### 3. Google Sheets Setup
```sql
-- 1. Create a Google Sheet and share with service account
-- 2. Update GOOGLE_SHEETS_SERVICE_ACCOUNT secret
-- 3. Update spreadsheet_id in procedure
-- 4. Run google/sheets/sheets_export.sql
-- 5. Test:
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.EXPORT_TO_GOOGLE_SHEETS(
    '[{"test": "data", "value": 123}]',
    'Test Export'
);
```

## 📋 Prerequisites

### For Email Tools:
- Snowflake account with ACCOUNTADMIN access
- Pre-approved email recipients list
- SYSADMIN role for procedure deployment

### For Google Integrations:
- Google Cloud Project
- Service Account with JSON credentials
- APIs enabled:
  - Google Docs API (for Docs export)
  - Google Sheets API (for Sheets export)
- Documents/Sheets shared with service account (Editor permissions)
- Snowflake external access configuration

## 🔧 Troubleshooting

### Email Not Sending
- Verify recipient is in ALLOWED_RECIPIENTS list
- Check notification integration is enabled

### Google API Errors
- Verify service account has Editor permissions
- Check API quotas (100 requests/100 seconds)
- Ensure document/sheet ID is correct

### Performance Issues
- Check Google API quotas
- Consider batching operations

## 📞 Support

For issues or questions:
- Review the troubleshooting section
- Consult Snowflake documentation