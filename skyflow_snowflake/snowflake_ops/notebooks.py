"""Stored procedure operations - Snowflake stored procedures for tokenization tasks."""

import time
from pathlib import Path
from typing import Optional, List
import snowflake.connector
from snowflake.connector.errors import Error as SnowflakeError
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from .client import SnowflakeClientWrapper

console = Console()


class StoredProcedureManager:
    """Manages Snowflake stored procedures for tokenization tasks."""
    
    def __init__(self, connection: snowflake.connector.SnowflakeConnection):
        self.connection = connection
        self.wrapper = SnowflakeClientWrapper(connection)
    
    def create_tokenization_procedure(self, prefix: str, substitutions: dict = None, batch_size: int = None) -> bool:
        """Create Snowflake stored procedure for tokenization using Python and real Skyflow API."""
        try:
            procedure_name = f"{prefix}_TOKENIZE_TABLE"
            database_name = f"{prefix}_database"
            
            # Get substitution values
            if substitutions is None:
                substitutions = {}
            
            vault_host = substitutions.get('SKYFLOW_VAULT_HOST', 'unknown')
            vault_id = substitutions.get('SKYFLOW_VAULT_ID', 'unknown') 
            skyflow_table = substitutions.get('SKYFLOW_TABLE', 'pii')
            table_column = substitutions.get('SKYFLOW_TABLE_COLUMN')
            
            # Create Python stored procedure that makes real Skyflow API calls
            procedure_sql = f"""
            CREATE OR REPLACE PROCEDURE {database_name}.PUBLIC.{procedure_name}()
            RETURNS STRING
            LANGUAGE PYTHON
            RUNTIME_VERSION = 3.12
            HANDLER = 'tokenize_table_handler'
            EXTERNAL_ACCESS_INTEGRATIONS = ({prefix}_SKYFLOW_EXTERNAL_ACCESS_INTEGRATION)
            SECRETS = ('skyflow_pat_token' = SKYFLOW_PAT_TOKEN)
            PACKAGES = ('requests', 'snowflake-snowpark-python')
            AS
            $$
import requests
import _snowflake
from snowflake.snowpark import Session

def tokenize_table_handler(snowpark_session):
    \"\"\"Tokenize PII data in customer table using real Skyflow API.\"\"\"
    
    try:
        # Get PAT token from secret, use substituted values for the rest
        pat_token = _snowflake.get_generic_secret_string('skyflow_pat_token')
        vault_host = "{vault_host}"
        vault_id = "{vault_id}" 
        skyflow_table = "{skyflow_table}"
        table_column = "{table_column}"
        
        # Configuration
        table_name = '{prefix}_customer_data'
        batch_size = {batch_size}
        
        # Skyflow API configuration (using correct vault host from SKYFLOW_VAULT_URL)
        tokenize_url = f"https://{{vault_host}}/v1/vaults/{{vault_id}}/{{skyflow_table}}"
        
        headers = {{
            "Authorization": f"Bearer {{pat_token}}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }}
        
        # Get total row count
        count_df = snowpark_session.sql(f"SELECT COUNT(*) as count FROM {{table_name}}")
        count_result = count_df.collect()
        total_rows = count_result[0]['COUNT']
        
        if total_rows == 0:
            return f"No data found in table {{table_name}}"
        
        processed = 0
        pii_columns = ['first_name', 'last_name', 'email', 'phone_number', 'address', 'date_of_birth']
        
        # Process in batches following Databricks pattern
        for offset in range(0, total_rows, batch_size):
            # Get batch of data
            batch_df = snowpark_session.sql(f\"\"\"
                SELECT customer_id, {{', '.join(pii_columns)}}
                FROM {{table_name}}
                ORDER BY customer_id
                LIMIT {{batch_size}} OFFSET {{offset}}
            \"\"\")
            
            batch_data = batch_df.collect()
            
            if not batch_data:
                continue  # Skip empty batches
            
            # For each PII column, tokenize the values
            for col in pii_columns:
                # Prepare records for tokenization (following Databricks pattern)
                skyflow_records = []
                row_mapping = {{}}
                
                for i, row in enumerate(batch_data):
                    # Check if row has the expected columns
                    if not hasattr(row, col.upper()) and col.upper() not in row:
                        continue
                    
                    value = row[col.upper()]  # Snowflake returns uppercase column names
                    if value is not None and str(value).strip() != '':
                        skyflow_records.append({{
                            "fields": {{table_column: str(value)}}
                        }})
                        row_mapping[len(skyflow_records) - 1] = (row['CUSTOMER_ID'], col)
                
                # Skip if no values to tokenize
                if not skyflow_records:
                    continue
                
                # Call Skyflow tokenization API
                payload = {{
                    "records": skyflow_records,
                    "tokenization": True
                }}
                
                response = requests.post(tokenize_url, json=payload, headers=headers, timeout=30)
                response.raise_for_status()
                
                result = response.json()
                
                # Debug: Log the actual API response structure
                if not result or 'records' not in result:
                    return f"Unexpected API response structure: {{str(result)[:300]}}"
                
                if 'records' in result:
                    # Update table with tokens
                    for record_idx, token_record in enumerate(result['records']):
                        if record_idx in row_mapping:
                            row_id, column_name = row_mapping[record_idx]
                            # Handle Skyflow tokenization response format
                            if 'tokens' in token_record and table_column in token_record['tokens']:
                                # Standard Skyflow tokenization response format
                                token = token_record['tokens'][table_column]
                            elif 'fields' in token_record and table_column in token_record['fields']:
                                # Alternative response format with fields wrapper
                                token = token_record['fields'][table_column]
                            elif 'token' in token_record:
                                # Direct token field
                                token = token_record['token']
                            elif table_column in token_record:
                                # Token under table column name
                                token = token_record[table_column]
                            else:
                                # Log the actual response structure for debugging
                                return f"Unexpected token record format: {{str(token_record)[:300]}} - Available keys: {{list(token_record.keys())}}"
                            
                            # Update the specific row and column with token
                            update_sql = f\"\"\"
                                UPDATE {{table_name}}
                                SET {{column_name}} = ?
                                WHERE customer_id = ?
                            \"\"\"
                            snowpark_session.sql(update_sql, [token, row_id]).collect()
                            
            
            processed += len(batch_data)
        
        return f"Tokenized {{processed}} rows in batches of {{batch_size}}"
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        return f"Tokenization failed: {{str(e)}} - Details: {{error_details[:500]}}"
$$
            """
            
            cursor = self.connection.cursor()
            
            def create_proc():
                cursor.execute(procedure_sql)
                return cursor.fetchall()
            
            self.wrapper.execute_with_retry(create_proc)
            cursor.close()
            console.print(f"✓ Created stored procedure: {procedure_name}")
            return True
            
        except SnowflakeError as e:
            console.print(f"✗ Failed to create stored procedure: {e}")
            return False
    
    def execute_tokenization_procedure(self, prefix: str) -> bool:
        """Execute the tokenization stored procedure."""
        try:
            database_name = f"{prefix}_database"
            procedure_name = f"{database_name}.PUBLIC.{prefix}_TOKENIZE_TABLE"
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Running tokenization procedure...", total=None)
                
                cursor = self.connection.cursor()
                
                def execute_proc():
                    cursor.execute(f"CALL {procedure_name}()")
                    result = cursor.fetchone()
                    return result[0] if result else "Completed"
                
                result_message = self.wrapper.execute_with_retry(execute_proc)
                cursor.close()
                
                progress.update(task, description=f"✓ Tokenization completed: {result_message}")
            
            console.print(f"✓ Tokenization procedure executed successfully")
            return True
            
        except SnowflakeError as e:
            console.print(f"✗ Failed to execute tokenization procedure: {e}")
            return False
    
    def setup_tokenization_procedure(self, prefix: str, substitutions: dict = None, batch_size: int = None) -> bool:
        """Setup and create the tokenization procedure."""
        return self.create_tokenization_procedure(prefix, substitutions, batch_size)
    
    def execute_tokenization_notebook(self, prefix: str, batch_size: int = None) -> bool:
        """Execute tokenization (renamed for compatibility).""" 
        return self.execute_tokenization_procedure(prefix)
    
    def drop_procedure(self, procedure_name: str) -> bool:
        """Drop a stored procedure."""
        try:
            cursor = self.connection.cursor()
            
            # Check if procedure exists
            cursor.execute(f"SHOW PROCEDURES LIKE '{procedure_name}'")
            if not cursor.fetchone():
                console.print(f"✓ Procedure '{procedure_name}' doesn't exist")
                cursor.close()
                return True
            
            def drop_proc():
                # Extract prefix from procedure name to build database name
                prefix = procedure_name.split('_TOKENIZE_TABLE')[0] if '_TOKENIZE_TABLE' in procedure_name else procedure_name
                database_name = f"{prefix}_database"
                cursor.execute(f"DROP PROCEDURE IF EXISTS {database_name}.PUBLIC.{procedure_name}()")
                return cursor.fetchall()
            
            self.wrapper.execute_with_retry(drop_proc)
            cursor.close()
            console.print(f"✓ Dropped procedure: {procedure_name}")
            return True
            
        except SnowflakeError as e:
            console.print(f"✗ Failed to drop procedure {procedure_name}: {e}")
            return False
    
    def delete_notebook(self, notebook_path: str) -> bool:
        """Delete a notebook (compatibility method - drops procedure instead)."""
        # Extract procedure name from path
        procedure_name = notebook_path.split('/')[-1] if '/' in notebook_path else notebook_path
        return self.drop_procedure(procedure_name)