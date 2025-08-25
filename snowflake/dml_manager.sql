/*
===============================================================================
DML PROCEDURES - PYTHON VERSION FOR SNOWFLAKE INTELLIGENCE
===============================================================================

PURPOSE:
Python-based stored procedures that can handle query results and return
formatted data for Snowflake Intelligence tools. These procedures can
process and return actual query results as formatted strings.

OVERVIEW:
- EXECUTE_DML_PY: Execute DML and return results/status

===============================================================================

/*
===============================================================================
EXECUTE DML - PYTHON VERSION
===============================================================================
Executes any DML statement and returns appropriate results.
*/

USE SCHEMA TOOLS;

CREATE OR REPLACE PROCEDURE EXECUTE_DML_PY(
    dml_statement STRING,
    output_format STRING DEFAULT 'table'  -- 'table', 'json', 'summary'
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'execute_dml'
COMMENT = 'Executes DML statements (SELECT, INSERT, UPDATE, DELETE, MERGE) with flexible output formatting. SELECT queries return data in table, JSON, or summary format. Other DML operations return execution status.'
AS
$$
import json

def execute_dml(session, dml_statement, output_format):
    try:
        # Validate input
        if not dml_statement or not dml_statement.strip():
            return "Error: DML statement cannot be empty"
        
        # Get statement type
        statement_type = dml_statement.strip().split()[0].upper()
        
        # Validate it's a DML statement
        if statement_type not in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE']:
            return f"Error: Only DML statements are allowed. Got: {statement_type}"
        
        # Execute the statement
        result = session.sql(dml_statement)
        
        if statement_type == 'SELECT':
            # For SELECT, collect and format results
            df = result.collect()
            columns = result.schema.names
            
            if output_format == 'json':
                rows = []
                for row in df:
                    row_dict = {columns[i]: str(row[i]) for i in range(len(columns))}
                    rows.append(row_dict)
                return json.dumps({
                    "statement": dml_statement,
                    "type": "SELECT",
                    "row_count": len(df),
                    "data": rows
                }, indent=2)
            
            elif output_format == 'summary':
                return f"SELECT executed successfully. Retrieved {len(df)} rows."
            
            else:  # table format
                output = f"Statement: {dml_statement}\n"
                output += f"Rows returned: {len(df)}\n\n"
                
                if len(df) == 0:
                    return output + "No data found."
                
                # Format as table (similar to QUERY_DATA_PY)
                col_widths = []
                for i, col in enumerate(columns):
                    max_width = len(col)
                    for row in df[:50]:
                        val_len = len(str(row[i]))
                        if val_len > max_width:
                            max_width = val_len
                    col_widths.append(min(max_width, 50))
                
                header = '| '
                separator = '|-'
                for i, col in enumerate(columns):
                    header += col.ljust(col_widths[i]) + ' | '
                    separator += '-' * col_widths[i] + '-|-'
                
                output += header + '\n'
                output += separator + '\n'
                
                for row in df[:100]:  # Limit display to 100 rows
                    row_str = '| '
                    for i, val in enumerate(row):
                        val_str = str(val)[:col_widths[i]]
                        row_str += val_str.ljust(col_widths[i]) + ' | '
                    output += row_str + '\n'
                
                if len(df) > 100:
                    output += f"\n... ({len(df) - 100} more rows)"
                
                return output
        else:
            # For other DML operations, get affected rows
            result.collect()  # Execute the statement
            return f"{statement_type} executed successfully. Use QUERY_HISTORY to check affected rows."
            
    except Exception as e:
        return f"Error executing DML: {str(e)}"
$$;