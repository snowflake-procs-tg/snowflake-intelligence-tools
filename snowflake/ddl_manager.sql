/*
===============================================================================
DDL PROCEDURES - PYTHON VERSION FOR SNOWFLAKE INTELLIGENCE
===============================================================================

PURPOSE:
Python-based stored procedures for Data Definition Language (DDL) operations.
These procedures manage database structure including creating, altering, and
dropping database objects like databases, schemas, tables, and views.

OVERVIEW:
- EXECUTE_DDL_PY: Execute any DDL statement with result formatting

===============================================================================
*/

/*
===============================================================================
EXECUTE DDL - PYTHON VERSION
===============================================================================
Executes any DDL statement and returns formatted results.
*/

USE SCHEMA TOOLS;

CREATE OR REPLACE PROCEDURE EXECUTE_DDL_PY(
    ddl_statement STRING,
    show_results BOOLEAN DEFAULT TRUE
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'execute_ddl'
COMMENT = 'Executes DDL statements (CREATE, ALTER, DROP, etc.) and formats results. For SHOW/DESCRIBE commands, returns formatted table output. For other DDL, returns execution status.'
AS
$$
def execute_ddl(session, ddl_statement, show_results):
    try:
        # Validate input
        if not ddl_statement or not ddl_statement.strip():
            return "Error: DDL statement cannot be empty"
        
        # Get statement type
        statement_type = ddl_statement.strip().split()[0].upper()
        
        # Common DDL keywords
        ddl_keywords = ['CREATE', 'ALTER', 'DROP', 'TRUNCATE', 'GRANT', 'REVOKE', 'SHOW', 'DESCRIBE', 'DESC']
        
        if statement_type not in ddl_keywords:
            return f"Error: Expected DDL statement, got: {statement_type}"
        
        # Execute the DDL
        result = session.sql(ddl_statement)
        
        # Handle SHOW and DESCRIBE commands differently
        if statement_type in ['SHOW', 'DESCRIBE', 'DESC'] and show_results:
            df = result.collect()
            columns = result.schema.names
            
            output = f"Statement: {ddl_statement}\n"
            output += f"Results: {len(df)} rows\n\n"
            
            if len(df) == 0:
                return output + "No objects found."
            
            # Format as table
            col_widths = []
            for i, col in enumerate(columns):
                max_width = len(col)
                for row in df[:50]:
                    val_len = len(str(row[i]) if row[i] is not None else 'NULL')
                    if val_len > max_width:
                        max_width = val_len
                col_widths.append(min(max_width, 40))
            
            # Header
            header = '| '
            separator = '|-'
            for i, col in enumerate(columns):
                header += col[:col_widths[i]].ljust(col_widths[i]) + ' | '
                separator += '-' * col_widths[i] + '-|-'
            
            output += header + '\n'
            output += separator + '\n'
            
            # Data rows
            for row in df:
                row_str = '| '
                for i, val in enumerate(row):
                    val_str = str(val) if val is not None else 'NULL'
                    val_str = val_str[:col_widths[i]]
                    row_str += val_str.ljust(col_widths[i]) + ' | '
                output += row_str + '\n'
            
            return output
        else:
            # For other DDL, just execute and return success
            result.collect()
            return f"DDL executed successfully: {ddl_statement}"
            
    except Exception as e:
        return f"Error executing DDL: {str(e)}"
$$;