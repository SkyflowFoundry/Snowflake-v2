"""Stored procedure operations - Snowflake stored procedures for tokenization tasks."""

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
        """Create Snowflake stored procedure using CTAS + SWAP approach for tokenization."""
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
            
            # Create Python stored procedure using CTAS + SWAP approach
            procedure_sql = f"""
            CREATE OR REPLACE PROCEDURE {database_name}.PUBLIC.{procedure_name}()
            RETURNS STRING
            LANGUAGE PYTHON
            RUNTIME_VERSION = 3.12
            HANDLER = 'ctas_swap_tokenize_handler'
            EXTERNAL_ACCESS_INTEGRATIONS = ({prefix}_SKYFLOW_EXTERNAL_ACCESS_INTEGRATION)
            SECRETS = ('skyflow_pat_token' = SKYFLOW_PAT_TOKEN)
            PACKAGES = ('requests', 'snowflake-snowpark-python')
            AS
            $$
import requests
import _snowflake
from snowflake.snowpark import Session

def ctas_swap_tokenize_handler(snowpark_session):
    # CTAS + SWAP approach for Skyflow tokenization - clean, atomic, scalable
    
    try:
        # Get configuration
        pat_token = _snowflake.get_generic_secret_string('skyflow_pat_token')
        vault_host = "{vault_host}"
        vault_id = "{vault_id}" 
        skyflow_table = "{skyflow_table}"
        table_column = "{table_column}"
        
        # Table configuration
        source_table = '{prefix}_database.PUBLIC.{prefix}_customer_data'
        snap_table = 'SNAP_CUSTOMER'  # temp table, unqualified
        staging_table = 'TOK_ALL'     # temp table, unqualified
        new_table = '{prefix}_database.PUBLIC.{prefix}_customer_data_NEW'
        
        batch_size = {batch_size}
        
        # Skyflow API configuration
        tokenize_url = f"https://{{vault_host}}/v1/vaults/{{vault_id}}/{{skyflow_table}}"
        headers = {{
            "Authorization": f"Bearer {{pat_token}}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }}
        
        # PII columns to tokenize
        pii_columns = ['first_name', 'last_name', 'email', 'phone_number', 'address', 'date_of_birth']
        
        # Step 1: Create deterministic snapshot with row numbers (using TRANSIENT)
        snapshot_sql = f\"\"\"
        CREATE OR REPLACE TRANSIENT TABLE {{snap_table}} AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY customer_id, email, total_purchases, created_at) AS rn,
            t.*
        FROM {{source_table}} t
        WHERE {{' OR '.join([f'{{col}} IS NOT NULL' for col in pii_columns])}}
        \"\"\"
        
        snowpark_session.sql(snapshot_sql).collect()
        
        # Get total rows to process
        count_df = snowpark_session.sql(f"SELECT COUNT(*) as count FROM {{snap_table}}")
        total_rows = count_df.collect()[0]['COUNT']
        
        if total_rows == 0:
            return "No rows to tokenize"
        
        # Step 2: Create staging table for tokens (using TRANSIENT)
        staging_columns = ', '.join([f'{{col}}_token STRING' for col in pii_columns])
        staging_sql = f\"\"\"
        CREATE OR REPLACE TRANSIENT TABLE {{staging_table}} (
            rn NUMBER,
            {{staging_columns}}
        )
        \"\"\"
        
        snowpark_session.sql(staging_sql).collect()
        
        # Step 3: Process each column's tokens by reading snapshot in rn order
        total_api_calls = 0
        total_tokens_processed = 0
        
        for col in pii_columns:
            # Get all non-null values for this column in rn order
            values_df = snowpark_session.sql(f\"\"\"
                SELECT rn, {{col}} as pii_value
                FROM {{snap_table}}
                WHERE {{col}} IS NOT NULL AND TRIM({{col}}) != ''
                ORDER BY rn
            \"\"\")
            
            values_data = values_df.collect()
            if not values_data:
                continue
                
            # Tokenize in batches and collect all tokens
            column_tokens = []  # List of (rn, token) tuples
            
            for batch_start in range(0, len(values_data), batch_size):
                batch_end = min(batch_start + batch_size, len(values_data))
                batch_data = values_data[batch_start:batch_end]
                
                # Prepare Skyflow API payload
                skyflow_records = []
                for row in batch_data:
                    skyflow_records.append({{
                        "fields": {{table_column: str(row['PII_VALUE'])}}
                    }})
                
                # Call Skyflow API
                payload = {{
                    "records": skyflow_records,
                    "tokenization": True
                }}
                
                response = requests.post(tokenize_url, json=payload, headers=headers, timeout=30)
                response.raise_for_status()
                result = response.json()
                
                if not result or 'records' not in result:
                    return f"Skyflow API error for {{col}}: " + str(result)[:200]
                
                # Extract tokens and pair with rn
                for i, token_record in enumerate(result['records']):
                    if i >= len(batch_data):
                        break
                        
                    rn = batch_data[i]['RN']
                    token = None
                    
                    # Extract token from response
                    if 'tokens' in token_record and table_column in token_record['tokens']:
                        token = token_record['tokens'][table_column]
                    elif 'fields' in token_record and table_column in token_record['fields']:
                        token = token_record['fields'][table_column]
                    elif 'token' in token_record:
                        token = token_record['token']
                    elif table_column in token_record:
                        token = token_record[table_column]
                    
                    if token:
                        column_tokens.append((rn, token))
                
                total_api_calls += 1
            
            # Insert tokens for this column into staging table
            if column_tokens:
                # Build INSERT statement with token data
                token_values = []
                for rn, token in column_tokens:
                    escaped_token = str(token).replace("'", "''")
                    token_values.append(f"({{rn}}, '{{escaped_token}}')")
                
                if token_values:
                    # Use MERGE to insert/update tokens by rn
                    merge_sql = f\"\"\"
                    MERGE INTO {{staging_table}} AS target
                    USING (
                        SELECT * FROM VALUES {{', '.join(token_values)}} AS t(rn, token)
                    ) AS source
                    ON target.rn = source.rn
                    WHEN MATCHED THEN UPDATE SET {{col}}_token = source.token
                    WHEN NOT MATCHED THEN INSERT (rn, {{col}}_token) VALUES (source.rn, source.token)
                    \"\"\"
                    
                    snowpark_session.sql(merge_sql).collect()
                    total_tokens_processed += len(column_tokens)
        
        # Step 4: Build new table with CTAS using COALESCE for token fallback
        coalesce_columns = []
        coalesce_columns.append('s.customer_id')
        
        for col in pii_columns:
            coalesce_columns.append(f'COALESCE(stg.{{col}}_token, s.{{col}}) AS {{col}}')
        
        # Add all other non-PII columns
        other_columns = [
            's.signup_date', 's.last_login', 's.total_purchases', 's.total_spent',
            's.loyalty_status', 's.preferred_language', 's.consent_marketing', 
            's.consent_data_sharing', 's.created_at', 's.updated_at'
        ]
        coalesce_columns.extend(other_columns)
        
        ctas_sql = f\"\"\"
        CREATE OR REPLACE TABLE {{new_table}} AS
        SELECT
            {{',\\n            '.join(coalesce_columns)}}
        FROM {{snap_table}} s
        LEFT JOIN {{staging_table}} stg USING (rn)
        \"\"\"
        
        snowpark_session.sql(ctas_sql).collect()
        
        # Step 5: Validation before SWAP
        validation_df = snowpark_session.sql(f\"\"\"
        SELECT 
            (SELECT COUNT(*) FROM {{source_table}}) AS old_rows,
            (SELECT COUNT(*) FROM {{new_table}}) AS new_rows
        \"\"\")
        
        validation_result = validation_df.collect()[0]
        old_rows = validation_result['OLD_ROWS']
        new_rows = validation_result['NEW_ROWS']
        
        if old_rows != new_rows:
            return f"Validation failed: row count mismatch ({{old_rows}} -> {{new_rows}})"
        
        # Step 6: Atomic SWAP
        swap_sql = f\"ALTER TABLE {{source_table}} SWAP WITH {{new_table}}\"
        snowpark_session.sql(swap_sql).collect()
        
        # Step 7: Drop the _NEW table (which now contains original plain-text data)
        try:
            snowpark_session.sql(f\"DROP TABLE IF EXISTS {{new_table}}\").collect()
        except Exception:
            pass  # Ignore cleanup errors
        
        # Step 8: Cleanup transient tables
        try:
            snowpark_session.sql(f\"DROP TABLE IF EXISTS {{snap_table}}\").collect()
            snowpark_session.sql(f\"DROP TABLE IF EXISTS {{staging_table}}\").collect()
        except Exception:
            pass  # Ignore cleanup errors
        
        return f"CTAS+SWAP tokenization complete: {{total_tokens_processed}} tokens via {{total_api_calls}} API calls ({{new_rows}} total rows)"
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        return f"CTAS+SWAP tokenization failed: {{str(e)}} - Details: {{error_details[:500]}}"
$$
            """
            
            cursor = self.connection.cursor()
            
            def create_proc():
                cursor.execute(procedure_sql)
                return cursor.fetchall()
            
            self.wrapper.execute_with_retry(create_proc)
            cursor.close()
            console.print(f"✓ Created CTAS+SWAP tokenization procedure: {procedure_name}")
            return True
            
        except SnowflakeError as e:
            console.print(f"✗ Failed to create stored procedure: {e}")
            return False
    
    def execute_tokenization_procedure(self, prefix: str) -> bool:
        """Execute the CTAS+SWAP tokenization stored procedure synchronously (blocking)."""
        try:
            database_name = f"{prefix}_database"
            procedure_name = f"{database_name}.PUBLIC.{prefix}_TOKENIZE_TABLE"
            
            cursor = self.connection.cursor()
            
            # Execute synchronously and wait for completion
            console.print(f"🚀 Starting CTAS+SWAP tokenization procedure: {procedure_name}")
            with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as progress:
                task = progress.add_task("Tokenizing data with CTAS+SWAP approach...", total=None)
                
                cursor.execute(f"CALL {procedure_name}()")
                result = cursor.fetchone()
                
                progress.update(task, description="✓ Tokenization completed")
            
            cursor.close()
            
            if result and result[0]:
                console.print(f"✓ CTAS+SWAP tokenization completed successfully")
                console.print(f"📋 Result: {result[0]}")
            else:
                console.print("⚠ Tokenization completed but no result returned")
            
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