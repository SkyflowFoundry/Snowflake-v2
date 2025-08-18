"""Snowflake connection wrapper with error handling."""

import time
from typing import Optional, Dict, Any
import snowflake.connector
from snowflake.connector.errors import Error as SnowflakeError
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()


class SnowflakeClientWrapper:
    """Enhanced Snowflake client with retry logic and better error handling."""
    
    def __init__(self, connection: snowflake.connector.SnowflakeConnection):
        self.connection = connection
    
    def wait_for_completion(self, operation_name: str, check_func, timeout: int = 300) -> bool:
        """Wait for an operation to complete with progress indication."""
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task(f"Waiting for {operation_name}...", total=None)
            
            start_time = time.time()
            while time.time() - start_time < timeout:
                try:
                    if check_func():
                        progress.update(task, description=f"✓ {operation_name} completed")
                        return True
                except Exception:
                    pass  # Continue waiting
                
                time.sleep(2)
            
            progress.update(task, description=f"✗ {operation_name} timed out")
            return False
    
    def execute_with_retry(self, operation, max_retries: int = 3, delay: int = 2) -> Any:
        """Execute operation with retry logic."""
        last_error = None
        
        for attempt in range(max_retries):
            try:
                return operation()
            except SnowflakeError as e:
                last_error = e
                if attempt < max_retries - 1:
                    console.print(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
                else:
                    console.print(f"All {max_retries} attempts failed")
        
        raise last_error
    
    def check_resource_exists(self, resource_type: str, check_func) -> bool:
        """Check if a resource exists without throwing errors."""
        try:
            check_func()
            return True
        except SnowflakeError as e:
            if "does not exist" in str(e) or "not found" in str(e).lower():
                return False
            raise  # Re-raise if it's not a "not found" error