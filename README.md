# Snowflake Tools Collection

A collection of tools and utilities for extending Snowflake's capabilities with external integrations and automation features. These tools are designed to work with Snowflake Intelligence and general Snowflake deployments.

**Note**: These examples are provided for reference and should be adapted for your specific use cases and security requirements.

## 📋 Table of Contents

- [Overview](#overview)
- [Available Tools](#available-tools)
  - [Snowflake Intelligence Setup](#snowflake-intelligence-setup)
  - [Email Tools](#email-tools)
  - [Google Workspace](#google-workspace)
  - [GitHub Integration](#github-integration)
  - [Database Management](#database-management)
- [Quick Start](#quick-start)
- [Prerequisites](#prerequisites)
- [Contributing](#contributing)

## 🎯 Overview

This repository provides Snowflake stored procedures and utilities for:

- **Snowflake Intelligence Extensions**: Custom tools for Snowflake Intelligence workflows
- **External Integrations**: Connect Snowflake with external services (Email, Google, GitHub)
- **Data Export**: Export data to various formats and platforms
- **Code Access**: Browse and retrieve code from GitHub repositories
- **Database Operations**: Manage DDL and DML operations systematically

## Available Tools

### 🤖 Snowflake Intelligence Setup
**Directory**: `snowflake_intelligence/`

Foundation setup for Snowflake Intelligence custom tools.

- **setup.sql**: Creates the SNOWFLAKE_INTELLIGENCE database and TOOLS schema
- Establishes the namespace for all custom procedures
- Required for deploying any of the tools in this repository

### 📧 Email Tools
**Directory**: `email/`

Send emails directly from Snowflake using native notification integration.

- **email_sender.sql**: Send emails with subject and body content
- Pre-approved recipient validation
- JSON response format

### 📄 Google Workspace
**Directory**: `google/`

Integration with Google Docs and Sheets for data export and reporting.

#### Google Docs
- **docs_export.sql**: Convert markdown to formatted Google Docs
- Support for headers, lists, and text formatting

#### Google Sheets  
- **sheets_export.sql**: Export JSON data to Google Sheets
- Automatic formatting and timestamped tabs
- Dynamic column detection

### 🐙 GitHub Integration
**Directory**: `github/`

Access GitHub repositories directly from Snowflake for code research and imports.

- **github_directory_lister.sql**: Browse repository file structure
- **github_file_extractor.sql**: Retrieve file contents
- **github_integration_setup.sql**: Configure GitHub API access
- Support for public and private repositories

Example:
```sql
-- List repository contents
CALL GITHUB_DIRECTORY_LISTER('microsoft', 'vscode', '', 'main');

-- Get a specific file
CALL GITHUB_FILE_EXTRACTOR('dbt-labs', 'jaffle_shop', 'models/staging/stg_orders.sql', 'main');
```

### 🗄️ Database Management
**Directory**: `snowflake/`

Utilities for database operations and management.

- **ddl_manager.sql**: Data Definition Language management
- **dml_manager.sql**: Data Manipulation Language operations

## 🚀 Quick Start

1. **Run the foundation setup** with `snowflake_intelligence/setup.sql`
2. **Choose the tools you need** from the directories above
3. **Run tool-specific setup scripts** (if applicable) with ACCOUNTADMIN role
4. **Configure credentials** and permissions as needed
5. **Deploy procedures** with SYSADMIN role
6. **Test the tools** with the provided examples

## 📋 Prerequisites

### General Requirements
- Snowflake account with appropriate roles (ACCOUNTADMIN for setup, SYSADMIN for deployment)
- External access capabilities enabled in Snowflake

### Tool-Specific Requirements

**Email Tools**:
- Notification integration configuration
- Pre-approved recipient list

**Google Workspace**:
- Google Cloud Project with APIs enabled
- Service Account credentials
- Proper sharing permissions

**GitHub Integration**:
- GitHub Personal Access Token
- Network rules for api.github.com

**Database Management**:
- Appropriate database permissions
- Understanding of DDL/DML operations

## 🤝 Contributing

Contributions are welcome! Please ensure:
- Code follows existing patterns
- Include documentation and examples
- Test thoroughly before submitting
- Follow security best practices

## 📄 License

See LICENSE file for details.

## ⚠️ Disclaimer

These tools are provided as examples and should be thoroughly tested and adapted for production use. Always follow your organization's security and compliance requirements.