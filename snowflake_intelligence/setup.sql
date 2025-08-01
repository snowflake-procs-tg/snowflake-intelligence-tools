/*
===============================================================================
SNOWFLAKE INTELLIGENCE SETUP SCRIPT
===============================================================================

This script sets up the complete infrastructure for Snowflake Intelligence,
including roles, warehouses, databases, schemas, and permissions. This script
is designed to be run end-to-end by users with ACCOUNTADMIN privileges.

WHAT THIS SCRIPT CREATES:
- Custom role: SNOWFLAKE_INTELLIGENCE_ADMIN_RL
- Dedicated warehouse: SNOWFLAKE_INTELLIGENCE_WH (optimized for AI workloads)
- Database: SNOWFLAKE_INTELLIGENCE
- Schemas: AGENTS, INTEGRATIONS, TOOLS
- Complete permission structure

PREREQUISITES:
- ACCOUNTADMIN role access
- Ability to create roles, warehouses, databases, and schemas

USAGE:
Run this script in its entirety from a Snowflake worksheet or SQL client.
The script will automatically grant the new role to the current user.

TABLE OF CONTENTS (Search for these markers):
- [SECTION_1_ROLES]       : Role creation and setup
- [SECTION_2_WAREHOUSE]   : Warehouse configuration
- [SECTION_3_DATABASE]    : Database and schema creation
- [SECTION_4_PERMISSIONS] : Permission grants and ownership

===============================================================================
*/

-- [SECTION_1_ROLES] Role Creation and Setup
-- =========================================
USE ROLE ACCOUNTADMIN;

-- Create dedicated admin role for Snowflake Intelligence
CREATE OR REPLACE ROLE SNOWFLAKE_INTELLIGENCE_ADMIN_RL 
    COMMENT = 'Administrative role for Snowflake Intelligence platform with full access to AI/ML resources';

-- [SECTION_2_WAREHOUSE] Warehouse Configuration  
-- ==============================================
USE ROLE SYSADMIN;

-- Create optimized warehouse for AI workloads and Cortex Search Service
CREATE OR REPLACE WAREHOUSE SNOWFLAKE_INTELLIGENCE_WH WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300              -- Suspend after 5 minutes of inactivity
    AUTO_RESUME = TRUE             -- Auto-resume on query execution
    MIN_CLUSTER_COUNT = 1          -- Minimum clusters for auto-scaling
    MAX_CLUSTER_COUNT = 1          -- Maximum clusters for auto-scaling
    SCALING_POLICY = 'STANDARD'    -- Standard scaling policy
    INITIALLY_SUSPENDED = TRUE     -- Start in suspended state
    COMMENT = 'Dedicated warehouse for Snowflake Intelligence AI workloads, Cortex Search, and query execution';

-- [SECTION_3_DATABASE] Database and Schema Creation
-- =================================================
-- Create main database for Snowflake Intelligence
CREATE OR REPLACE DATABASE SNOWFLAKE_INTELLIGENCE
    COMMENT = 'Main database for Snowflake Intelligence platform containing agents, tools, and integrations';

-- Create organized schemas for different components
CREATE OR REPLACE SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS
    COMMENT = 'Schema for AI agents';

CREATE OR REPLACE SCHEMA SNOWFLAKE_INTELLIGENCE.INTEGRATIONS  
    COMMENT = 'Schema for external service integrations, secrets, and API configurations';

CREATE OR REPLACE SCHEMA SNOWFLAKE_INTELLIGENCE.TOOLS
    COMMENT = 'Schema for custom tools which are functions, and stored procedures';

-- [SECTION_4_PERMISSIONS] Permission Grants and Ownership
-- ========================================================

-- Grant the new role to the current user automatically
DECLARE
    SQL_COMMAND STRING;
BEGIN
    SQL_COMMAND := 'GRANT ROLE SNOWFLAKE_INTELLIGENCE_ADMIN_RL TO USER "' || CURRENT_USER() || '";';
    EXECUTE IMMEDIATE SQL_COMMAND;
    RETURN 'Role SNOWFLAKE_INTELLIGENCE_ADMIN_RL granted successfully to user ' || CURRENT_USER();
END;

-- Transfer ownership of all created objects to the admin role
GRANT OWNERSHIP ON DATABASE SNOWFLAKE_INTELLIGENCE TO ROLE SNOWFLAKE_INTELLIGENCE_ADMIN_RL;
GRANT OWNERSHIP ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE SNOWFLAKE_INTELLIGENCE_ADMIN_RL;
GRANT OWNERSHIP ON SCHEMA SNOWFLAKE_INTELLIGENCE.TOOLS TO ROLE SNOWFLAKE_INTELLIGENCE_ADMIN_RL;
GRANT OWNERSHIP ON SCHEMA SNOWFLAKE_INTELLIGENCE.INTEGRATIONS TO ROLE SNOWFLAKE_INTELLIGENCE_ADMIN_RL;
GRANT OWNERSHIP ON WAREHOUSE SNOWFLAKE_INTELLIGENCE_WH TO ROLE SNOWFLAKE_INTELLIGENCE_ADMIN_RL;

-- Grant the admin role to SYSADMIN for role hierarchy
GRANT ROLE SNOWFLAKE_INTELLIGENCE_ADMIN_RL TO ROLE SYSADMIN;

-- Display setup completion message
SELECT 
    'Snowflake Intelligence setup completed successfully!' AS status,
    'Role: SNOWFLAKE_INTELLIGENCE_ADMIN_RL' AS role_created,
    'Warehouse: SNOWFLAKE_INTELLIGENCE_WH' AS warehouse_created,
    'Database: SNOWFLAKE_INTELLIGENCE' AS database_created,
    'Schemas: AGENTS, INTEGRATIONS, TOOLS' AS schemas_created;


