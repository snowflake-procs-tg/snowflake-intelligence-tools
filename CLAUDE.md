# CLAUDE.md

## Purpose of This Document

This document serves as a context guide for Claude Code (claude.ai/code) when working with this repository. It provides:
- An overview of the codebase structure and purpose
- Key commands and workflows specific to this project
- Architecture decisions and integration patterns
- Development guidelines and best practices

By reading this document, Claude Code can better understand the project context and provide more accurate, relevant assistance when you're developing or maintaining code in this repository.

**Important**: All code in this repository is for demonstration and educational purposes only. These examples showcase integration patterns but should be thoroughly tested and adapted before use in any real-world scenarios.

## Project Overview

This repository contains demonstration Snowflake stored procedures that showcase how to extend Snowflake Intelligence with external integration capabilities. These example tools demonstrate:
- Email notifications via Snowflake's native notification integration
- Google Docs export with markdown-to-formatted-document conversion
- Google Sheets export with JSON data formatting and timestamped tabs

## Key Commands

### Testing Stored Procedures
```sql
-- Test email sending
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.SEND_EMAIL(
    'recipient@example.com',
    'Test Subject',
    'Test message body'
);

-- Test Google Docs export
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.EXPORT_TO_GOOGLE_DOCS(
    '# Test Document\n\nThis is a test.',
    'YOUR_DOC_ID'
);

-- Test Google Sheets export
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.EXPORT_TO_GOOGLE_SHEETS(
    '[{"col1": "value1", "col2": "value2"}]',
    'Test Sheet'
);
```

### Local Python Development
```bash
# Install Google API dependencies for local testing
pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib

# Test authentication with service account
python -c "from google.oauth2 import service_account; print('Auth module loaded')"
```

## Architecture Notes

### Repository Structure
- `/email/` - Email notification tools
  - `email_sender.sql` - Core email sending stored procedure
- `/google/docs/` - Google Docs integration
  - `docs_export.sql` - Markdown to Google Docs export procedure
- `/google/sheets/` - Google Sheets integration  
  - `sheets_export.sql` - JSON to Google Sheets export procedure
- `/snowflake_intelligence/` - Database setup
  - `setup.sql` - Initial database and schema creation
- `/_adding_event_table/` - Event table setup documentation (reference only)

### Google API Integration Patterns
The stored procedures use these Google API resources:
- **Sheets API v4**:
  - `spreadsheets.values.update` - Writing data to sheets
  - `spreadsheets.batchUpdate` - Formatting and sheet operations
  - `spreadsheets.create` - Creating new spreadsheets
- **Docs API v1**:
  - `documents.get` - Reading document structure
  - `documents.batchUpdate` - Writing formatted content
- **Drive API v3**:
  - `files.update` - Modifying file metadata
  - `permissions.create` - Sharing documents

### Authentication Patterns
1. **Service Account (Recommended for Snowflake UDF/Stored Procedures)**:
   - Store credentials securely in Snowflake
   - Pass credentials to UDF/Stored Procedure at runtime
   
2. **Required Scopes for Google Sheets and Drive**:
   - `https://www.googleapis.com/auth/spreadsheets` (read/write sheets)
   - `https://www.googleapis.com/auth/drive` (folder creation and file management)
   - `https://www.googleapis.com/auth/spreadsheets.readonly` (read-only sheets)

### Snowflake Integration Requirements
- **External Access**: All procedures require configured external access integrations
- **Secrets Management**: Service account credentials stored as Snowflake secrets
- **Network Rules**: Specific rules for googleapis.com endpoints
- **Package Dependencies**: google-api-python-client specified in procedure definitions
- **Notification Integration**: Required for email functionality

## Development Workflow

### Adding New Features
1. Review existing procedures for patterns and conventions
2. Test API interactions locally with Python
3. Create stored procedure following established patterns
4. Ensure proper error handling and JSON response format
5. Test in Snowflake with appropriate permissions

### Modifying Existing Procedures
1. Check for dependencies in Snowflake Intelligence
2. Maintain backward compatibility with response formats
3. Test with various input scenarios
4. Update any related documentation

### Security Considerations
- Never hardcode credentials in procedures
- Always use Snowflake secrets for sensitive data
- Validate all inputs before API calls
- Follow principle of least privilege for service accounts