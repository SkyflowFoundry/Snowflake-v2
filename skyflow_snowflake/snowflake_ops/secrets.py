"""Secrets management - Snowflake secrets functionality for Skyflow integration."""

from typing import Dict, List
import snowflake.connector
from snowflake.connector.errors import Error as SnowflakeError
from rich.console import Console
from .client import SnowflakeClientWrapper

console = Console()


class SnowflakeSecretsManager:
    """Manages Snowflake secrets for external function authentication."""
    
    def __init__(self, connection: snowflake.connector.SnowflakeConnection):
        self.connection = connection
        self.wrapper = SnowflakeClientWrapper(connection)
    
    def create_secret(self, secret_name: str, secret_value: str, comment: str = None) -> bool:
        """Create a Snowflake secret object."""
        try:
            cursor = self.connection.cursor()
            
            # Check if secret already exists
            cursor.execute(f"SHOW SECRETS LIKE '{secret_name}'")
            if cursor.fetchone():
                console.print(f"✓ Secret '{secret_name}' already exists")
                cursor.close()
                return True
            
            # Create secret
            comment_clause = f"COMMENT = '{comment}'" if comment else ""
            create_sql = f"""
            CREATE SECRET {secret_name}
            TYPE = GENERIC_STRING
            SECRET_STRING = '{secret_value}'
            {comment_clause}
            """
            
            def create_secret_obj():
                cursor.execute(create_sql)
                return cursor.fetchall()
            
            self.wrapper.execute_with_retry(create_secret_obj)
            cursor.close()
            console.print(f"✓ Created secret: {secret_name}")
            return True
            
        except SnowflakeError as e:
            console.print(f"✗ Failed to create secret {secret_name}: {e}")
            return False
    
    def alter_secret(self, secret_name: str, new_value: str) -> bool:
        """Update an existing Snowflake secret."""
        try:
            cursor = self.connection.cursor()
            
            def alter_secret_obj():
                cursor.execute(f"ALTER SECRET {secret_name} SET SECRET_STRING = '{new_value}'")
                return cursor.fetchall()
            
            self.wrapper.execute_with_retry(alter_secret_obj)
            cursor.close()
            console.print(f"✓ Updated secret: {secret_name}")
            return True
            
        except SnowflakeError as e:
            console.print(f"✗ Failed to update secret {secret_name}: {e}")
            return False
    
    def drop_secret(self, secret_name: str, database_name: str = None) -> bool:
        """Drop a Snowflake secret."""
        try:
            cursor = self.connection.cursor()
            
            # If database name provided, use qualified name to avoid database context issues
            if database_name:
                qualified_name = f"{database_name}.PUBLIC.{secret_name}"
                try:
                    cursor.execute(f"SHOW SECRETS LIKE '{secret_name}' IN DATABASE {database_name}")
                except SnowflakeError:
                    # Database might not exist, secret doesn't exist
                    console.print(f"✓ Secret '{secret_name}' doesn't exist")
                    cursor.close()
                    return True
            else:
                qualified_name = secret_name
                cursor.execute(f"SHOW SECRETS LIKE '{secret_name}'")
            
            if not cursor.fetchone():
                console.print(f"✓ Secret '{secret_name}' doesn't exist")
                cursor.close()
                return True
            
            def drop_secret_obj():
                cursor.execute(f"DROP SECRET IF EXISTS {qualified_name}")
                return cursor.fetchall()
            
            self.wrapper.execute_with_retry(drop_secret_obj)
            cursor.close()
            console.print(f"✓ Dropped secret: {secret_name}")
            return True
            
        except SnowflakeError as e:
            console.print(f"✗ Failed to drop secret {secret_name}: {e}")
            return False
    
    def setup_skyflow_secrets(self, skyflow_config: Dict[str, str]) -> bool:
        """Setup only the sensitive Skyflow secret (PAT token)."""
        # Only create secret for the sensitive PAT token
        # Other values will be substituted directly in SQL templates
        secrets = {
            "SKYFLOW_PAT_TOKEN": (skyflow_config["pat_token"], "Skyflow PAT token for API authentication")
        }
        
        success = True
        for secret_name, (value, comment) in secrets.items():
            if not self.create_secret(secret_name, value, comment):
                success = False
        
        return success
    
    def list_secrets(self, pattern: str = None) -> List[str]:
        """List all secrets, optionally filtered by pattern."""
        try:
            cursor = self.connection.cursor()
            if pattern:
                cursor.execute(f"SHOW SECRETS LIKE '{pattern}'")
            else:
                cursor.execute("SHOW SECRETS")
            
            results = cursor.fetchall()
            cursor.close()
            return [row[1] for row in results] if results else []  # Second column is the secret name
        except SnowflakeError:
            return []
    
    def verify_secrets(self, required_secrets: List[str]) -> bool:
        """Verify that all required secrets exist."""
        existing_secrets = self.list_secrets()
        missing_secrets = [secret for secret in required_secrets if secret not in existing_secrets]
        
        if missing_secrets:
            console.print(f"✗ Missing secrets: {', '.join(missing_secrets)}")
            return False
        
        console.print(f"✓ All required secrets exist")
        return True
    
    def secret_exists(self, secret_name: str) -> bool:
        """Check if a secret exists."""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SHOW SECRETS LIKE '{secret_name}'")
            exists = cursor.fetchone() is not None
            cursor.close()
            return exists
        except SnowflakeError:
            return False