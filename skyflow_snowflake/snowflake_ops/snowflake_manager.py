"""Snowflake database and schema operations - Snowflake resource management for Skyflow integration."""

from typing import Dict, Optional, List
import snowflake.connector
from snowflake.connector.errors import Error as SnowflakeError
from rich.console import Console
from .client import SnowflakeClientWrapper

console = Console()


class SnowflakeResourceManager:
    """Manages Snowflake database, schema, and external function resources for Skyflow integration."""
    
    def __init__(self, connection: snowflake.connector.SnowflakeConnection):
        self.connection = connection
        self.wrapper = SnowflakeClientWrapper(connection)
    
    def create_api_integration(self, name: str, api_provider: str, api_key_secret: str) -> bool:
        """Create Snowflake API integration for external functions."""
        try:
            cursor = self.connection.cursor()
            
            # Check if integration already exists
            cursor.execute(f"SHOW API INTEGRATIONS LIKE '{name}'")
            if cursor.fetchone():
                console.print(f"✓ API integration '{name}' already exists")
                cursor.close()
                return True
            
            # Create API integration
            create_sql = f"""
            CREATE OR REPLACE API INTEGRATION {name}
            API_PROVIDER = {api_provider}
            API_KEY = '{api_key_secret}'
            ENABLED = TRUE
            """
            
            def create_integration():
                cursor.execute(create_sql)
                return cursor.fetchall()
            
            self.wrapper.execute_with_retry(create_integration)
            cursor.close()
            console.print(f"✓ Created API integration: {name}")
            return True
            
        except SnowflakeError as e:
            console.print(f"✗ Failed to create API integration {name}: {e}")
            return False
    
    def create_database(self, name: str, comment: Optional[str] = None) -> bool:
        """Create Snowflake database."""
        try:
            cursor = self.connection.cursor()
            
            # Check if database already exists
            cursor.execute(f"SHOW DATABASES LIKE '{name}'")
            if cursor.fetchone():
                console.print(f"✓ Database '{name}' already exists")
                cursor.close()
                return True
            
            # Create database
            comment_clause = f"COMMENT = '{comment or f'Skyflow integration database - {name}'}'"
            create_sql = f"CREATE DATABASE {name} {comment_clause}"
            
            def create_db():
                cursor.execute(create_sql)
                return cursor.fetchall()
            
            self.wrapper.execute_with_retry(create_db)
            cursor.close()
            console.print(f"✓ Created database: {name}")
            return True
            
        except SnowflakeError as e:
            console.print(f"✗ Failed to create database {name}: {e}")
            return False
    
    def create_schema(self, database_name: str, schema_name: str = "PUBLIC") -> bool:
        """Create schema in Snowflake database."""
        try:
            cursor = self.connection.cursor()
            
            # Switch to the database
            cursor.execute(f"USE DATABASE {database_name}")
            
            # Check if schema already exists
            cursor.execute(f"SHOW SCHEMAS LIKE '{schema_name}'")
            if cursor.fetchone():
                console.print(f"✓ Schema '{database_name}.{schema_name}' already exists")
                cursor.close()
                return True
            
            # Create schema
            def create_sch():
                cursor.execute(f"CREATE SCHEMA {schema_name}")
                return cursor.fetchall()
            
            self.wrapper.execute_with_retry(create_sch)
            cursor.close()
            console.print(f"✓ Created schema: {database_name}.{schema_name}")
            return True
            
        except SnowflakeError as e:
            console.print(f"✗ Failed to create schema {database_name}.{schema_name}: {e}")
            return False
    
    def create_role(self, role_name: str, comment: Optional[str] = None) -> bool:
        """Create Snowflake role if it doesn't exist."""
        try:
            cursor = self.connection.cursor()
            
            # Check if role already exists
            cursor.execute(f"SHOW ROLES LIKE '{role_name}'")
            if cursor.fetchone():
                console.print(f"✓ Role '{role_name}' already exists")
                cursor.close()
                return True
            
            # Create role
            comment_clause = f"COMMENT = '{comment or f'Role for Skyflow integration - {role_name}'}'"
            create_sql = f"CREATE ROLE {role_name} {comment_clause}"
            
            def create_role_obj():
                cursor.execute(create_sql)
                return cursor.fetchall()
            
            self.wrapper.execute_with_retry(create_role_obj)
            cursor.close()
            console.print(f"✓ Created role: {role_name}")
            return True
            
        except SnowflakeError as e:
            console.print(f"✗ Failed to create role {role_name}: {e}")
            return False
    
    def create_required_roles(self, roles: List[str], group_config=None) -> bool:
        """Create multiple roles needed for the integration."""
        success = True
        
        console.print(f"Creating {len(roles)} required roles...")
        
        for role_name in roles:
            # Determine role type from role name to provide appropriate description
            comment = f'Role for {role_name}'  # Default
            
            if group_config:
                role_upper = role_name.upper()
                if group_config.plain_text_groups.upper() in role_upper:
                    comment = 'Users who can see detokenized (plain text) PII data'
                elif group_config.masked_groups.upper() in role_upper:
                    comment = 'Users who see masked/redacted PII data'  
                elif group_config.redacted_groups.upper() in role_upper:
                    comment = 'Users who only see tokenized PII data'
            
            if not self.create_role(role_name, comment):
                success = False
        
        if success:
            console.print("✓ All required roles created successfully")
            # Grant roles to current user so they appear in UI
            self._grant_roles_to_current_user(roles)
        else:
            console.print("⚠ Some role creation failed - check permissions")
            
        return success
    
    def _grant_roles_to_current_user(self, roles: List[str]) -> bool:
        """Grant roles to current user so they appear in Snowflake UI."""
        try:
            cursor = self.connection.cursor()
            current_user = None
            
            # Get current user
            cursor.execute("SELECT CURRENT_USER()")
            result = cursor.fetchone()
            if result:
                current_user = result[0]
            
            if not current_user:
                console.print("⚠ Could not determine current user for role grants")
                return False
            
            # Grant each role to current user
            for role_name in roles:
                try:
                    grant_sql = f"GRANT ROLE {role_name} TO USER {current_user}"
                    cursor.execute(grant_sql)
                    console.print(f"  ✓ Granted {role_name} to {current_user}")
                except Exception as e:
                    console.print(f"  ⚠ Failed to grant {role_name} to {current_user}: {e}")
            
            cursor.close()
            return True
            
        except Exception as e:
            console.print(f"⚠ Error granting roles to current user: {e}")
            return False
    
    def grant_database_access_to_roles(self, database_name: str, roles: List[str]) -> bool:
        """Grant database access to the created roles."""
        try:
            cursor = self.connection.cursor()
            success = True
            
            console.print(f"Granting database access to {len(roles)} roles...")
            
            for role_name in roles:
                try:
                    # Grant USAGE on database
                    def grant_usage():
                        cursor.execute(f"GRANT USAGE ON DATABASE {database_name} TO ROLE {role_name}")
                        return cursor.fetchall()
                    
                    self.wrapper.execute_with_retry(grant_usage)
                    
                    # Grant USAGE on schema
                    def grant_schema_usage():
                        cursor.execute(f"GRANT USAGE ON SCHEMA {database_name}.PUBLIC TO ROLE {role_name}")
                        return cursor.fetchall()
                    
                    self.wrapper.execute_with_retry(grant_schema_usage)
                    
                    # Grant SELECT on tables (for querying data)
                    def grant_select():
                        cursor.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {database_name}.PUBLIC TO ROLE {role_name}")
                        return cursor.fetchall()
                    
                    self.wrapper.execute_with_retry(grant_select)
                    
                    console.print(f"  ✓ Granted access to role: {role_name}")
                    
                except SnowflakeError as e:
                    console.print(f"  ✗ Failed to grant access to role {role_name}: {e}")
                    success = False
            
            cursor.close()
            
            if success:
                console.print("✓ Database access granted to all roles")
            
            return success
            
        except SnowflakeError as e:
            console.print(f"✗ Failed to grant database access: {e}")
            return False
    
    def drop_database(self, name: str, cascade: bool = True) -> bool:
        """Drop Snowflake database and all contents."""
        try:
            cursor = self.connection.cursor()
            
            # Check if database exists
            cursor.execute(f"SHOW DATABASES LIKE '{name}'")
            if not cursor.fetchone():
                console.print(f"✓ Database '{name}' doesn't exist")
                cursor.close()
                return True
            
            # Drop database
            cascade_clause = "CASCADE" if cascade else ""
            def drop_db():
                cursor.execute(f"DROP DATABASE {name} {cascade_clause}")
                return cursor.fetchall()
            
            self.wrapper.execute_with_retry(drop_db)
            cursor.close()
            console.print(f"✓ Dropped database: {name}")
            return True
            
        except SnowflakeError as e:
            console.print(f"✗ Failed to drop database {name}: {e}")
            return False
    
    def drop_api_integration(self, name: str) -> bool:
        """Drop Snowflake API integration."""
        try:
            cursor = self.connection.cursor()
            
            # Check if integration exists
            cursor.execute(f"SHOW API INTEGRATIONS LIKE '{name}'")
            if not cursor.fetchone():
                console.print(f"✓ API integration '{name}' doesn't exist")
                cursor.close()
                return True
            
            def drop_integration():
                cursor.execute(f"DROP API INTEGRATION {name}")
                return cursor.fetchall()
            
            self.wrapper.execute_with_retry(drop_integration)
            cursor.close()
            console.print(f"✓ Dropped API integration: {name}")
            return True
            
        except SnowflakeError as e:
            console.print(f"✗ Failed to drop API integration {name}: {e}")
            return False
    
    def setup_skyflow_integration(self, vault_url: str, vault_id: str, secret_name: str) -> bool:
        """Setup Skyflow API integration and external functions."""
        success = True
        
        # Create API integration for Skyflow
        success &= self.create_api_integration(
            name="SKYFLOW_API_INTEGRATION",
            api_provider="'AWS_API_GATEWAY'",  # or appropriate provider
            api_key_secret=secret_name
        )
        
        return success
    
    def database_exists(self, name: str) -> bool:
        """Check if a database exists."""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SHOW DATABASES LIKE '{name}'")
            exists = cursor.fetchone() is not None
            cursor.close()
            return exists
        except SnowflakeError:
            return False
    
    def api_integration_exists(self, name: str) -> bool:
        """Check if an API integration exists."""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SHOW API INTEGRATIONS LIKE '{name}'")
            exists = cursor.fetchone() is not None
            cursor.close()
            return exists
        except SnowflakeError:
            return False