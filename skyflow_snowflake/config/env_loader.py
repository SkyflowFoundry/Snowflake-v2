"""Environment configuration loader for Snowflake Skyflow integration."""

import os
from pathlib import Path
from typing import Dict, Optional, Any
from dotenv import load_dotenv


class EnvLoader:
    """Loads and processes environment variables from .env.local file."""
    
    def __init__(self, env_file: str = ".env.local"):
        self.env_file = env_file
        self._load_env_file()
    
    def _load_env_file(self) -> None:
        """Load environment file if it exists."""
        env_path = Path(self.env_file)
        if env_path.exists():
            print(f"Loading configuration from {self.env_file}...")
            load_dotenv(env_path)
        else:
            print(f"Warning: {self.env_file} not found - using environment variables only")
    
    def get_snowflake_config(self) -> Dict[str, Optional[str]]:
        """Extract Snowflake configuration from environment."""
        return {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "pat_token": os.getenv("SNOWFLAKE_PAT_TOKEN"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema_name": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
            "role": os.getenv("SNOWFLAKE_ROLE")
        }
    
    def get_skyflow_config(self) -> Dict[str, Any]:
        """Extract Skyflow configuration from environment."""
        return {
            "vault_url": os.getenv("SKYFLOW_VAULT_URL"),
            "vault_id": os.getenv("SKYFLOW_VAULT_ID"),
            "pat_token": os.getenv("SKYFLOW_PAT_TOKEN"),
            "table": os.getenv("SKYFLOW_TABLE"),
            "table_column": os.getenv("SKYFLOW_TABLE_COLUMN", "pii_values"),
            "batch_size": int(os.getenv("SKYFLOW_BATCH_SIZE", "25"))
        }
    
    def get_group_mappings(self) -> Dict[str, str]:
        """Extract group mappings for detokenization."""
        return {
            "plain_text_groups": os.getenv("PLAIN_TEXT_GROUPS", "auditor"),
            "masked_groups": os.getenv("MASKED_GROUPS", "customer_service"),
            "redacted_groups": os.getenv("REDACTED_GROUPS", "marketing")
        }
    
    def validate_config(self) -> Dict[str, bool]:
        """Validate that required configuration is present."""
        snowflake = self.get_snowflake_config()
        skyflow = self.get_skyflow_config()
        
        # Check that either password or PAT token is provided for authentication
        has_auth = (snowflake["password"] is not None) or (snowflake["pat_token"] is not None)
        
        return {
            "snowflake_account": snowflake["account"] is not None,
            "snowflake_user": snowflake["user"] is not None,
            "snowflake_auth": has_auth,  # Either password or PAT token required
            "snowflake_warehouse": snowflake["warehouse"] is not None,
            "snowflake_database": snowflake["database"] is not None,
            "skyflow_vault_url": skyflow["vault_url"] is not None,
            "skyflow_vault_id": skyflow["vault_id"] is not None,
            "skyflow_pat_token": skyflow["pat_token"] is not None,
            "skyflow_table": skyflow["table"] is not None
        }