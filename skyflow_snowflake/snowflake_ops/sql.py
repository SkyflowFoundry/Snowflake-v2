"""SQL execution - Snowflake SQL execution functionality for Skyflow integration."""

import time
from pathlib import Path
from typing import Dict, Optional, List, Any, Tuple
import snowflake.connector
from snowflake.connector.errors import Error as SnowflakeError
from rich.console import Console
from rich.table import Table
from .client import SnowflakeClientWrapper

console = Console()


class SnowflakeSQLExecutor:
    """Executes SQL files and statements against Snowflake."""
    
    def __init__(self, connection: snowflake.connector.SnowflakeConnection):
        self.connection = connection
        self.wrapper = SnowflakeClientWrapper(connection)
    
    def apply_substitutions(self, sql: str, substitutions: Dict[str, str]) -> str:
        """Apply variable substitutions to SQL content."""
        if not substitutions:
            return sql
        
        for key, value in substitutions.items():
            sql = sql.replace(f"${{{key}}}", str(value))
        
        return sql
    
    def execute_statement(self, sql: str) -> Optional[Tuple[List[Any], List[str]]]:
        """Execute a single SQL statement and return results and column names."""
        try:
            cursor = self.connection.cursor()
            
            def execute():
                cursor.execute(sql)
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                return results, columns
            
            results, columns = self.wrapper.execute_with_retry(execute)
            cursor.close()
            return results, columns
                
        except SnowflakeError as e:
            console.print(f"✗ SQL execution error: {e}")
            return None
    
    def execute_sql_file(self, file_path: str, substitutions: Optional[Dict[str, str]] = None) -> bool:
        """Execute SQL from a file with variable substitutions."""
        # If path is relative, look in templates directory
        if not Path(file_path).is_absolute():
            template_dir = Path(__file__).parent.parent / "templates"
            path = template_dir / file_path
        else:
            path = Path(file_path)
        
        if not path.exists():
            console.print(f"✗ SQL file not found: {path}")
            return False
        
        console.print(f"Executing SQL file: {path.name}")
        
        try:
            with open(path, 'r') as f:
                sql_content = f.read()
            
            # Apply substitutions
            if substitutions:
                sql_content = self.apply_substitutions(sql_content, substitutions)
            
            # Split into individual statements and extract SQL from mixed comment/SQL parts
            raw_statements = sql_content.split(';')
            statements = []
            for stmt in raw_statements:
                cleaned = stmt.strip()
                if not cleaned or len(cleaned) <= 2:
                    continue
                    
                # Check if this contains Python code (preserve original formatting)
                if '$$' in cleaned and ('def ' in cleaned or 'import ' in cleaned):
                    # This is a Python function block - preserve original formatting
                    lines = cleaned.split('\n')
                    sql_lines = []
                    for line in lines:
                        # Only skip pure comment lines, preserve Python code indentation
                        if line.strip() and not line.strip().startswith('--'):
                            sql_lines.append(line.rstrip())  # Keep original indentation, remove trailing spaces
                    
                    if sql_lines:
                        sql_statement = '\n'.join(sql_lines)
                        if len(sql_statement.strip()) > 2:
                            statements.append(sql_statement)
                else:
                    # Regular SQL - process line by line and clean up
                    lines = cleaned.split('\n')
                    sql_lines = []
                    for line in lines:
                        line = line.strip()
                        # Skip empty lines and comment-only lines
                        if line and not line.startswith('--'):
                            sql_lines.append(line)
                    
                    # If we found actual SQL content, preserve formatting for Python functions
                    if sql_lines:
                        # For multi-line statements, preserve line breaks
                        # For single-line statements, join with spaces for better readability
                        if len(sql_lines) > 3:  # Multi-line statement, preserve formatting
                            sql_statement = '\n'.join(sql_lines).strip()
                        else:  # Single-line statement, join with spaces
                            sql_statement = ' '.join(sql_lines).strip()
                        
                        if len(sql_statement) > 2:
                            statements.append(sql_statement)
            
            success = True
            for i, statement in enumerate(statements):
                console.print(f"  Executing statement {i+1}/{len(statements)}")
                result = self.execute_statement(statement)
                
                if result is None:
                    success = False
                    console.print(f"✗ Failed to execute statement {i+1}")
                    break
                else:
                    console.print(f"  ✓ Statement {i+1} completed")
            
            if success:
                console.print(f"✓ Successfully executed {path.name}")
            
            return success
            
        except Exception as e:
            console.print(f"✗ Error reading/executing {file_path}: {e}")
            return False
    
    def execute_query_with_results(self, sql: str, max_rows: int = 100) -> Optional[List[Dict[str, Any]]]:
        """Execute a query and return results."""
        result = self.execute_statement(sql)
        
        if result:
            results_data, columns = result
            if results_data and columns:
                # Convert to list of dictionaries
                results = []
                
                for row in results_data[:max_rows]:
                    row_dict = {columns[i]: row[i] for i in range(len(columns))} if columns else {}
                    results.append(row_dict)
                
                return results
        
        return None
    
    def verify_table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        try:
            # Parse table name to get parts
            parts = table_name.split('.')
            if len(parts) == 3:
                database, schema, table = parts
                sql = f"SHOW TABLES LIKE '{table}' IN SCHEMA {database}.{schema}"
            elif len(parts) == 2:
                schema, table = parts
                sql = f"SHOW TABLES LIKE '{table}' IN SCHEMA {schema}"
            else:
                table = parts[0]
                sql = f"SHOW TABLES LIKE '{table}'"
            
            result = self.execute_statement(sql)
            return result is not None and len(result[0]) > 0
        except:
            return False
    
    def verify_function_exists(self, function_name: str) -> bool:
        """Check if a function exists."""
        try:
            parts = function_name.split('.')
            if len(parts) == 3:
                database, schema, func = parts
                sql = f"SHOW FUNCTIONS LIKE '{func}' IN SCHEMA {database}.{schema}"
            elif len(parts) == 2:
                schema, func = parts
                sql = f"SHOW FUNCTIONS LIKE '{func}' IN SCHEMA {schema}"
            else:
                func = parts[0]
                sql = f"SHOW FUNCTIONS LIKE '{func}'"
            
            result = self.execute_statement(sql)
            return result is not None and len(result[0]) > 0
        except:
            return False
    
    def get_table_row_count(self, table_name: str) -> Optional[int]:
        """Get row count for a table."""
        sql = f"SELECT COUNT(*) as count FROM {table_name}"
        results = self.execute_query_with_results(sql)
        
        if results and len(results) > 0:
            count_value = results[0].get('count', 0)
            # Convert to int if it's a string
            return int(count_value) if count_value is not None else 0
        
        return None
    
    def show_table_sample(self, table_name: str, limit: int = 5) -> None:
        """Display a sample of table data."""
        sql = f"SELECT * FROM {table_name} LIMIT {limit}"
        results = self.execute_query_with_results(sql, max_rows=limit)
        
        if results:
            table = Table(title=f"Sample data from {table_name}")
            
            # Add columns
            if results:
                for column in results[0].keys():
                    table.add_column(column)
                
                # Add rows
                for row in results:
                    table.add_row(*[str(value) for value in row.values()])
            
            console.print(table)
        else:
            console.print(f"No data found in {table_name}")