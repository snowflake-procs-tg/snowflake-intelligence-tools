-- GitHub API Integration Setup for Snowflake
-- This file contains all necessary setup steps for GitHub API access

-- =============================================================================
-- TABLE OF CONTENTS
-- =============================================================================
-- 1. PREREQUISITES
--    - ACCOUNTADMIN role required for initial setup
--    - GitHub Personal Access Token (PAT) needed
--
-- 2. NETWORK CONFIGURATION
--    - Create network rule for GitHub API endpoint
--
-- 3. SECRET MANAGEMENT
--    - Store GitHub token securely in Snowflake
--
-- 4. EXTERNAL ACCESS INTEGRATION
--    - Create integration combining network rule and secret
--
-- 5. PERMISSIONS
--    - Grant necessary permissions to SYSADMIN role
--
-- 6. VALIDATION
--    - Test commands to verify setup
-- =============================================================================

-- =============================================================================
-- STEP 1: NETWORK RULE CONFIGURATION
-- =============================================================================
-- This must be run by ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Create network rule to allow outbound connections to GitHub API
CREATE OR REPLACE NETWORK RULE SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_API_NETWORK_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('api.github.com:443');

-- =============================================================================
-- STEP 2: SECRET STORAGE
-- =============================================================================
-- Store your GitHub Personal Access Token securely
-- Replace the token below with your actual GitHub PAT
-- To create a token: GitHub Settings > Developer Settings > Personal Access Tokens

CREATE OR REPLACE SECRET SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_TOKEN
    TYPE = GENERIC_STRING
    SECRET_STRING = 'YOUR_GITHUB_PERSONAL_ACCESS_TOKEN_HERE';

-- Note: Required GitHub token scopes:
-- - repo (for private repositories)
-- - public_repo (for public repositories only)

-- =============================================================================
-- STEP 3: EXTERNAL ACCESS INTEGRATION
-- =============================================================================
-- Create the integration that combines network access and authentication

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GITHUB_API_INTEGRATION
    ALLOWED_NETWORK_RULES = (SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_API_NETWORK_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (SNOWFLAKE_INTELLIGENCE.TOOLS.GITHUB_TOKEN)
    ENABLED = TRUE;

-- =============================================================================
-- STEP 4: GRANT PERMISSIONS
-- =============================================================================
-- Allow SYSADMIN role to use the integration

GRANT USAGE ON INTEGRATION GITHUB_API_INTEGRATION TO ROLE SYSADMIN;

-- Grant access to the database and schema if needed
GRANT USAGE ON DATABASE SNOWFLAKE_INTELLIGENCE TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA SNOWFLAKE_INTELLIGENCE.TOOLS TO ROLE SYSADMIN;
GRANT CREATE PROCEDURE ON SCHEMA SNOWFLAKE_INTELLIGENCE.TOOLS TO ROLE SYSADMIN;

-- =============================================================================
-- STEP 5: VALIDATION
-- =============================================================================
-- Verify the setup by checking the integration status

SHOW INTEGRATIONS LIKE 'GITHUB_API_INTEGRATION';
DESC INTEGRATION GITHUB_API_INTEGRATION;

-- Switch to SYSADMIN to verify permissions
USE ROLE SYSADMIN;
USE DATABASE SNOWFLAKE_INTELLIGENCE;
USE SCHEMA TOOLS;

-- The procedures can now be created using the GITHUB_API_INTEGRATION

-- =============================================================================
-- TROUBLESHOOTING
-- =============================================================================
-- If you encounter issues:
--
-- 1. Authentication errors (401):
--    - Verify your GitHub token is valid
--    - Check token hasn't expired
--    - Ensure token has required scopes
--
-- 2. Network errors:
--    - Verify network rule is properly configured
--    - Check external access integration is ENABLED
--
-- 3. Permission errors:
--    - Ensure SYSADMIN has USAGE on the integration
--    - Verify database and schema permissions
--
-- 4. Rate limiting (403):
--    - GitHub API has rate limits
--    - Authenticated requests: 5,000 per hour
--    - Consider implementing caching or batching
-- =============================================================================