-- GitHub Directory Lister using GraphQL API
-- Lists files and directories in a GitHub repository path

-- =============================================================================
-- TABLE OF CONTENTS
-- =============================================================================
-- 1. PROCEDURE DEFINITION
--    - Parameters: OWNER, REPO, PATH, BRANCH
--    - Returns: VARIANT (JSON structure)
--    - Uses GitHub GraphQL API for efficient data retrieval
--
-- 2. IMPLEMENTATION DETAILS
--    - GraphQL queries for root and subdirectories
--    - Error handling for authentication, rate limits, and network issues
--    - Response parsing and organization
--
-- 3. PERMISSIONS
--    - Grant usage to SYSADMIN role
--
-- 4. EXAMPLE USAGE
--    - List root directory
--    - Browse specific directories
--    - Explore popular repositories (VSCode, pandas, dbt, etc.)
--
-- 5. OUTPUT STRUCTURE
--    - JSON response format with files and directories
--    - Metadata including file sizes and types
-- =============================================================================

-- PREREQUISITES: Run GITHUB_INTEGRATION_SETUP.sql first to configure API access

USE ROLE SYSADMIN;
USE DATABASE SNOWFLAKE_INTELLIGENCE;
USE SCHEMA TOOLS;

CREATE OR REPLACE PROCEDURE GITHUB_DIRECTORY_LISTER(
    OWNER VARCHAR,
    REPO VARCHAR,
    PATH VARCHAR DEFAULT '',
    BRANCH VARCHAR DEFAULT 'main'
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'github_directory_lister'
EXTERNAL_ACCESS_INTEGRATIONS = (GITHUB_API_INTEGRATION)
SECRETS = ('github_token' = SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_TOKEN)
AS
$$
import json
import requests
import _snowflake
from typing import Dict, Any, List

def github_directory_lister(session, owner: str, repo: str, 
                           path: str = '', branch: str = 'main') -> dict:
    """
    Lists files and directories in a GitHub repository path using the GraphQL API.
    
    Args:
        session: Snowflake session object
        owner: Repository owner/organization
        repo: Repository name
        path: Directory path in repository (empty for root)
        branch: Branch name (default: 'main')
    
    Returns:
        Dict containing directory listing or error information
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
    
    # GraphQL query to list directory contents
    # For root directory, we need to use a different query structure
    if not path or path == '/':
        query = """
        query ListRootDirectory($owner: String!, $repo: String!, $branch: String!) {
            repository(owner: $owner, name: $repo) {
                defaultBranchRef: ref(qualifiedName: $branch) {
                    target {
                        ... on Commit {
                            tree {
                                entries {
                                    name
                                    type
                                    object {
                                        ... on Blob {
                                            byteSize
                                            isBinary
                                        }
                                        ... on Tree {
                                            entries {
                                                name
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """
        variables = {
            "owner": owner,
            "repo": repo,
            "branch": f"refs/heads/{branch}"
        }
    else:
        query = """
        query ListDirectory($owner: String!, $repo: String!, $expression: String!) {
            repository(owner: $owner, name: $repo) {
                object(expression: $expression) {
                    ... on Tree {
                        entries {
                            name
                            type
                            object {
                                ... on Blob {
                                    byteSize
                                    isBinary
                                }
                                ... on Tree {
                                    entries {
                                        name
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """
        expression = f"{branch}:{path}"
        variables = {
            "owner": owner,
            "repo": repo,
            "expression": expression
        }
    
    # Prepare the request payload (variables already set above)
    payload = {
        "query": query,
        "variables": variables
    }
    
    try:
        # Make the GraphQL request
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
        
        # Extract repository data
        repo_data = result.get("data", {}).get("repository")
        if not repo_data:
            return {
                "error": "Repository not found",
                "message": f"Repository {owner}/{repo} not found or not accessible"
            }
        
        # Handle different response structures for root vs subdirectory
        if not path or path == '/':
            # Root directory query has different structure
            ref_data = repo_data.get("defaultBranchRef")
            if not ref_data or not ref_data.get("target"):
                return {
                    "error": "Branch not found",
                    "message": f"Branch '{branch}' not found",
                    "branch": branch
                }
            tree_object = ref_data["target"].get("tree")
            if not tree_object:
                return {
                    "error": "Tree not found",
                    "message": f"Could not access repository tree for branch '{branch}'",
                    "branch": branch
                }
            entries = tree_object.get("entries", [])
        else:
            # Subdirectory query
            tree_object = repo_data.get("object")
            if not tree_object:
                return {
                    "error": "Path not found",
                    "message": f"Path '{path}' not found in branch '{branch}'",
                    "path": path,
                    "branch": branch
                }
            entries = tree_object.get("entries", [])
        
        # Organize results
        files = []
        directories = []
        
        for entry in entries:
            entry_info = {
                "name": entry["name"],
                "type": entry["type"].lower()
            }
            
            if entry["type"] == "blob":
                # It's a file
                if entry.get("object"):
                    entry_info["size_bytes"] = entry["object"].get("byteSize", 0)
                    entry_info["is_binary"] = entry["object"].get("isBinary", False)
                files.append(entry_info)
            elif entry["type"] == "tree":
                # It's a directory
                if entry.get("object") and entry["object"].get("entries"):
                    entry_info["item_count"] = len(entry["object"]["entries"])
                else:
                    entry_info["item_count"] = 0
                directories.append(entry_info)
        
        # Sort results
        files.sort(key=lambda x: x["name"].lower())
        directories.sort(key=lambda x: x["name"].lower())
        
        # Return successful result
        return {
            "success": True,
            "repository": f"{owner}/{repo}",
            "branch": branch,
            "path": path if path else "/",
            "total_items": len(entries),
            "file_count": len(files),
            "directory_count": len(directories),
            "directories": directories,
            "files": files,
            "url": f"https://github.com/{owner}/{repo}/tree/{branch}/{path}" if path else f"https://github.com/{owner}/{repo}/tree/{branch}"
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
GRANT USAGE ON PROCEDURE GITHUB_DIRECTORY_LISTER(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE SYSADMIN;

-- =============================================================================
-- EXAMPLE USAGE: Listing Repository Contents
-- =============================================================================

-- 1. List root directory of a repository
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_DIRECTORY_LISTER(
    'microsoft',
    'vscode',
    '',        -- Empty string for root
    'main'
);

-- 2. List contents of src directory
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_DIRECTORY_LISTER(
    'sfc-gh-tgordonjr',
    'frost-forecast',
    'pipelines/',
    'main'
);

-- 3. Browse Snowflake examples
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_DIRECTORY_LISTER(
    'Snowflake-Labs',
    'sfguide-data-engineering-with-snowpark-python',
    'lab',
    'main'
);

-- 4. Explore pandas source structure
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_DIRECTORY_LISTER(
    'pandas-dev',
    'pandas',
    'pandas/core',
    'main'
);

-- 5. Check dbt project structure
CALL SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_DIRECTORY_LISTER(
    'dbt-labs',
    'jaffle_shop',
    'models',
    'main'
);

-- =============================================================================
-- OUTPUT STRUCTURE:
-- =============================================================================
-- {
--   "success": true,
--   "repository": "owner/repo",
--   "branch": "main",
--   "path": "/src",
--   "total_items": 25,
--   "file_count": 20,
--   "directory_count": 5,
--   "directories": [
--     {"name": "components", "type": "tree", "item_count": 15},
--     {"name": "utils", "type": "tree", "item_count": 8}
--   ],
--   "files": [
--     {"name": "index.js", "type": "blob", "size_bytes": 2456, "is_binary": false},
--     {"name": "logo.png", "type": "blob", "size_bytes": 45678, "is_binary": true}
--   ]
-- }
-- =============================================================================