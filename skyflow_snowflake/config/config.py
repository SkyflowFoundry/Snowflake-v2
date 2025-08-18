"""Main configuration class for Snowflake Skyflow integration."""

import os
from typing import Dict, Optional
from pydantic import BaseModel, ValidationError, model_validator
import snowflake.connector
from .env_loader import EnvLoader

# Disable cloud metadata service calls to prevent warnings
os.environ['NO_PROXY'] = '169.254.169.254'


class SnowflakeConfig(BaseModel):
    """Snowflake configuration model."""
    account: str
    user: str
    warehouse: str
    database: str
    schema_name: str = "PUBLIC"
    role: Optional[str] = None
    # Authentication - use either password OR pat_token OR oauth
    password: Optional[str] = None
    pat_token: Optional[str] = None
    oauth_token: Optional[str] = None


class SkyflowConfig(BaseModel):
    """Skyflow configuration model."""
    vault_url: str
    vault_id: str
    pat_token: str
    table: str
    table_column: str = "pii_values"  # Column name in Skyflow table
    batch_size: int = 25  # Default batch size


class GroupConfig(BaseModel):
    """Group mapping configuration."""
    plain_text_groups: str = "auditor"
    masked_groups: str = "customer_service"
    redacted_groups: str = "marketing"


class SetupConfig:
    """Main configuration manager for Snowflake Skyflow setup."""
    
    def __init__(self, env_file: str = ".env.local"):
        self.env_loader = EnvLoader(env_file)
        self._snowflake_config: Optional[SnowflakeConfig] = None
        self._skyflow_config: Optional[SkyflowConfig] = None
        self._group_config: Optional[GroupConfig] = None
        self._connection: Optional[snowflake.connector.SnowflakeConnection] = None
    
    @property
    def snowflake(self) -> SnowflakeConfig:
        """Get Snowflake configuration."""
        if self._snowflake_config is None:
            config_data = self.env_loader.get_snowflake_config()
            try:
                self._snowflake_config = SnowflakeConfig(**config_data)
            except ValidationError as e:
                raise ValueError(f"Invalid Snowflake configuration: {e}")
        return self._snowflake_config
    
    @property
    def skyflow(self) -> SkyflowConfig:
        """Get Skyflow configuration.""" 
        if self._skyflow_config is None:
            config_data = self.env_loader.get_skyflow_config()
            try:
                self._skyflow_config = SkyflowConfig(**config_data)
            except ValidationError as e:
                raise ValueError(f"Invalid Skyflow configuration: {e}")
        return self._skyflow_config
    
    @property
    def groups(self) -> GroupConfig:
        """Get group configuration."""
        if self._group_config is None:
            config_data = self.env_loader.get_group_mappings()
            self._group_config = GroupConfig(**config_data)
        return self._group_config
    
    @property
    def connection(self) -> 'snowflake.connector.SnowflakeConnection':
        """Get authenticated Snowflake connection."""
        if self._connection is None:
            config = self.snowflake
            
            # Build connection parameters with network optimization
            conn_params = {
                'account': config.account,
                'user': config.user,
                'warehouse': config.warehouse,
                'database': config.database,
                'schema': config.schema_name,
                'role': config.role,
                # Network timeout and retry parameters
                'login_timeout': 120,  # Wait longer for network policy resolution
                'network_timeout': 120,  # Extended timeout for network issues
                'socket_timeout': 60,   # Socket-level timeout
                'ocsp_fail_open': True,  # Allow connections if OCSP check fails
                # Disable cloud metadata service detection to avoid warnings
                'disable_request_pooling': True,
                'client_request_mfa_token': False
            }
            
            # Add authentication method
            if config.oauth_token:
                # OAuth authentication - sometimes bypasses network policies
                conn_params['authenticator'] = 'oauth'
                conn_params['token'] = config.oauth_token
            elif config.pat_token:
                # For PAT tokens, use password field with special format
                conn_params['password'] = config.pat_token
            elif config.password:
                conn_params['password'] = config.password
            else:
                raise ValueError("Either SNOWFLAKE_PASSWORD, SNOWFLAKE_PAT_TOKEN, or SNOWFLAKE_OAUTH_TOKEN must be provided")
            
            self._connection = snowflake.connector.connect(**conn_params)
        return self._connection
    
    def validate(self) -> None:
        """Validate all configuration is present and correct."""
        validation = self.env_loader.validate_config()
        missing = [key for key, valid in validation.items() if not valid]
        
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
        
        # Test Snowflake connection
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT CURRENT_USER()")
            cursor.fetchone()
            cursor.close()
        except Exception as e:
            raise ValueError(f"Failed to authenticate with Snowflake: {e}")
        
        print("✓ Configuration validated successfully")
    
    def get_substitutions(self, prefix: str) -> Dict[str, str]:
        """Get variable substitutions for SQL templates."""
        return {
            "PREFIX": prefix,
            "DATABASE": self.snowflake.database,
            "SCHEMA": self.snowflake.schema_name,
            "WAREHOUSE": self.snowflake.warehouse,
            # Add prefix-specific database name for SQL templates
            f"{prefix}_database".upper(): f"{prefix}_database",
            "PREFIX_DATABASE": f"{prefix}_database",
            "SKYFLOW_VAULT_URL": self.skyflow.vault_url,
            "SKYFLOW_VAULT_ID": self.skyflow.vault_id,
            "SKYFLOW_VAULT_HOST": self.skyflow.vault_url.replace('https://', '').replace('http://', ''),
            "SKYFLOW_TABLE": self.skyflow.table,
            "SKYFLOW_TABLE_COLUMN": getattr(self.skyflow, 'table_column', 'pii_values'),
            "PLAIN_TEXT_GROUPS": self.groups.plain_text_groups,
            "MASKED_GROUPS": self.groups.masked_groups,
            "REDACTED_GROUPS": self.groups.redacted_groups
        }