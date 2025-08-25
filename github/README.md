# GitHub Tools for Snowflake Intelligence

Simple tools to access GitHub repositories directly from Snowflake, enabling you to browse repositories, read code, and integrate GitHub data into your workflows.

## What These Tools Do

- **github_directory_lister.sql** - Lists files and folders in any GitHub repository
  - Browse repository structure
  - See file sizes and types
  - Navigate directories
  
- **github_file_extractor.sql** - Gets the content of any file from GitHub
  - Read source code
  - Access documentation
  - Pull configuration files

## Why Use These Tools?

- **Research Code Examples** - Study implementations from popular open-source projects
- **Import Configurations** - Pull dbt models, SQL scripts, or config files directly
- **Analyze Codebases** - Combine with Snowflake's analytics to understand code patterns
- **Documentation Access** - Read README files and documentation without leaving Snowflake

## Setup (3 Steps)

1. **Get a GitHub Personal Access Token:**
   - Go to GitHub → Settings → Developer Settings → Personal Access Tokens
   - Click "Generate new token (classic)"
   - Select scope: `public_repo` (for public repos) or `repo` (includes private repos)
   - Copy the generated token

2. **Run the setup script:**
   ```sql
   -- Run github_integration_setup.sql as ACCOUNTADMIN
   -- Replace YOUR_GITHUB_PERSONAL_ACCESS_TOKEN_HERE with your actual token
   ```

3. **Create the procedures:**
   ```sql
   -- Run as SYSADMIN
   -- Run github_directory_lister.sql
   -- Run github_file_extractor.sql
   ```

## Usage Examples

### List files in a repository root
```sql
CALL GITHUB_DIRECTORY_LISTER('microsoft', 'vscode', '', 'main');
```

### Browse a specific directory
```sql
CALL GITHUB_DIRECTORY_LISTER('dbt-labs', 'jaffle_shop', 'models', 'main');
```

### Get a file's content
```sql
CALL GITHUB_FILE_EXTRACTOR('dbt-labs', 'jaffle_shop', 'models/staging/stg_orders.sql', 'main');
```

### Read a Python implementation
```sql
CALL GITHUB_FILE_EXTRACTOR('pandas-dev', 'pandas', 'pandas/core/frame.py', 'main');
```

## Output Format

Both procedures return JSON with either:
- **Success**: Data with metadata (repository, branch, path, content/files)
- **Error**: Clear error message with details for troubleshooting

## Common Use Cases

1. **Learning from Examples**
   - Study how popular projects implement features
   - Find code patterns and best practices
   
2. **Importing SQL/Code**
   - Pull dbt models into your project
   - Copy SQL procedures or functions
   
3. **Configuration Management**
   - Access YAML/JSON configs from repositories
   - Sync settings across environments

## Limitations

- Cannot access binary files (images, compiled code)
- Subject to GitHub API rate limits (5,000 requests/hour with token)
- Requires network access to api.github.com

## Requirements

- ACCOUNTADMIN role (for initial setup only)
- SYSADMIN role (for running the tools)
- GitHub Personal Access Token
- Snowflake external access capability