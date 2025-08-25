-- GitHub File Extractor using GraphQL API
-- Fetches file content from GitHub repositories

-- =============================================================================
-- TABLE OF CONTENTS
-- =============================================================================
-- 1. PROCEDURE DEFINITION
--    - Parameters: OWNER, REPO, FILE_PATH, BRANCH
--    - Returns: VARIANT (JSON structure with file content)
--    - Uses GitHub GraphQL API for efficient retrieval
--
-- 2. IMPLEMENTATION DETAILS
--    - GraphQL query for file content
--    - Error handling (auth, rate limits, binary files)
--    - Response parsing and validation
--
-- 3. EXAMPLE USAGE
--    - Python files (pandas, Snowflake examples)
--    - SQL files (dbt models, Snowflake procedures)
--    - Configuration files (YAML, JSON)
--    - Various popular repositories
--
-- 4. OUTPUT STRUCTURE
--    - JSON response with file content and metadata
--    - Error messages for various failure scenarios
--
-- 5. TIPS FOR EXPLORING CODE
--    - Best practices for using the extractor
--    - Integration patterns and use cases
-- =============================================================================

-- PREREQUISITES: Run GITHUB_INTEGRATION_SETUP.sql first to configure API access

USE ROLE SYSADMIN;
USE DATABASE SNOWFLAKE_INTELLIGENCE;
USE SCHEMA TOOLS;

CREATE OR REPLACE PROCEDURE GITHUB_FILE_EXTRACTOR(
    OWNER VARCHAR,
    REPO VARCHAR,
    FILE_PATH VARCHAR,
    BRANCH VARCHAR DEFAULT 'main'
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'github_file_extractor'
EXTERNAL_ACCESS_INTEGRATIONS = (GITHUB_API_INTEGRATION)
SECRETS = ('github_token' = SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_TOKEN)
AS
$$
import json
import requests
import _snowflake
from typing import Dict, Any, Optional

def github_file_extractor(session, owner: str, repo: str, 
                          file_path: str, branch: str = 'main') -> dict:
    """
    Fetches a file from a GitHub repository using the GraphQL API.
    Token is now stored securely in Snowflake secrets.
    
    Args:
        session: Snowflake session object
        owner: Repository owner/organization
        repo: Repository name
        file_path: Path to file in repository
        branch: Branch name (default: 'main')
    
    Returns:
        Dict containing file content and metadata or error information
    """
    
    # Get the GitHub token from Snowflake secrets
    github_token = _snowflake.get_generic_secret_string('github_token')
    
    # GitHub GraphQL endpoint
    url = "https://api.github.com/graphql"
    
    # Set up headers with authentication
    headers = {
        "Authorization": f"Bearer {github_token}",
        "Content-Type": "application/json"
    }
    
    # GraphQL query to fetch file content
    query = """
    query GetFileContent($owner: String!, $repo: String!, $expression: String!) {
        repository(owner: $owner, name: $repo) {
            object(expression: $expression) {
                ... on Blob {
                    text
                    byteSize
                    isBinary
                }
            }
        }
    }
    """
    
    # Build the expression (branch:path)
    expression = f"{branch}:{file_path}"
    
    # Set up variables for the query
    variables = {
        "owner": owner,
        "repo": repo,
        "expression": expression
    }
    
    # Prepare the request payload
    payload = {
        "query": query,
        "variables": variables
    }
    
    try:
        # Make the GraphQL request with timeout
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        
        # Check for HTTP errors
        if response.status_code == 401:
            return {
                "error": "Authentication failed",
                "message": "Invalid or expired GitHub token",
                "status_code": 401
            }
        elif response.status_code == 403:
            return {
                "error": "Rate limit exceeded",
                "message": "GitHub API rate limit reached. Try again later.",
                "status_code": 403
            }
        elif response.status_code != 200:
            return {
                "error": "Request failed",
                "message": f"HTTP {response.status_code}: {response.text}",
                "status_code": response.status_code
            }
        
        # Parse the response
        result = response.json()
        
        # Check for GraphQL errors
        if "errors" in result:
            return {
                "error": "GraphQL error",
                "message": result["errors"][0].get("message", "Unknown error"),
                "errors": result["errors"]
            }
        
        # Extract file data
        repo_data = result.get("data", {}).get("repository")
        if not repo_data:
            return {
                "error": "Repository not found",
                "message": f"Repository {owner}/{repo} not found or not accessible"
            }
        
        file_object = repo_data.get("object")
        if not file_object:
            return {
                "error": "File not found",
                "message": f"File '{file_path}' not found in branch '{branch}'",
                "path": file_path,
                "branch": branch
            }
        
        # Check if file is binary
        if file_object.get("isBinary", False):
            return {
                "error": "Binary file",
                "message": "Cannot retrieve binary files via GraphQL API",
                "path": file_path,
                "byte_size": file_object.get("byteSize", 0),
                "is_binary": True
            }
        
        # Return successful result
        return {
            "success": True,
            "repository": f"{owner}/{repo}",
            "branch": branch,
            "path": file_path,
            "content": file_object.get("text", ""),
            "byte_size": file_object.get("byteSize", 0),
            "is_binary": False,
            "url": f"https://github.com/{owner}/{repo}/blob/{branch}/{file_path}"
        }
        
    except requests.exceptions.ConnectionError as e:
        return {
            "error": "Connection error",
            "message": f"Failed to connect to GitHub API: {str(e)}",
            "url": url,
            "details": "Check if external access integration is properly configured"
        }
    except requests.exceptions.Timeout:
        return {
            "error": "Timeout",
            "message": "Request to GitHub API timed out after 30 seconds"
        }
    except Exception as e:
        return {
            "error": "Unexpected error",
            "message": str(e),
            "type": type(e).__name__,
            "url": url
        }
$$;

-- Grant usage permissions
GRANT USAGE ON PROCEDURE GITHUB_FILE_EXTRACTOR(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;

-- =============================================================================
-- EXAMPLE USAGE: Exploring Code with GitHub File Extractor
-- =============================================================================

-- 1. Fetch a Python implementation file (pandas DataFrame)
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_FILE_EXTRACTOR(
    'pandas-dev',              -- Owner: pandas team
    'pandas',                  -- Repository
    'pandas/core/frame.py',    -- DataFrame implementation
    'main'
);

-- 2. Look at Snowflake's example code
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_FILE_EXTRACTOR(
    'Snowflake-Labs',
    'sfguide-data-engineering-with-snowpark-python',
    'lab/07_daily_city_metrics_update_sp/app.py',
    'main'
);

-- 3. Examine SQL parsing logic from SQLGlot
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_FILE_EXTRACTOR(
    'tobymao',
    'sqlglot',
    'sqlglot/dialects/snowflake.py',  -- Snowflake SQL dialect implementation
    'main'
);

-- 4. Study machine learning implementations
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_FILE_EXTRACTOR(
    'sfc-gh-tgordonjr',
    'frost-forecast',
    'pipelines/frost_forecast_cortex.sql',
    'main'
);

-- 5. Review database connection patterns
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_FILE_EXTRACTOR(
    'snowflakedb',
    'snowflake-connector-python',
    'src/snowflake/connector/connection.py',
    'main'
);

-- 6. Explore Apache Airflow DAG examples
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_FILE_EXTRACTOR(
    'apache',
    'airflow',
    'airflow/example_dags/example_branch_operator.py',
    'main'
);

-- 7. Check out dbt model examples
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_FILE_EXTRACTOR(
    'dbt-labs',
    'jaffle_shop',
    'models/staging/stg_orders.sql',
    'main'
);

-- 8. Review configuration files (YAML example)
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_FILE_EXTRACTOR(
    'dbt-labs',
    'dbt-core',
    'dbt_project.yml',
    'main'
);

-- 9. Examine test implementations
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_FILE_EXTRACTOR(
    'pytest-dev',
    'pytest',
    'testing/test_assertion.py',
    'main'
);

-- 10. Look at API implementations
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_FILE_EXTRACTOR(
    'tiangolo',
    'fastapi',
    'fastapi/applications.py',
    'master'  -- Note: FastAPI uses 'master' not 'main'
);

-- =============================================================================
-- TIPS FOR EXPLORING CODE:
-- =============================================================================
-- 1. Use this to study implementation patterns from well-known libraries
-- 2. Extract code snippets for learning or reference
-- 3. Compare different approaches across similar projects
-- 4. Review best practices from popular open-source projects
-- 5. Find examples of specific algorithms or design patterns
-- 
-- Note: The content is returned in the 'content' field of the JSON response
-- You can save it to a table or use it with other Snowflake procedures for analysis
-- =============================================================================