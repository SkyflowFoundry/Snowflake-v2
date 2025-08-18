-- Snowflake External Functions Setup for Skyflow Integration
-- Uses Python external functions with external access integration for Skyflow API calls
-- Provides role-based data access using Snowflake RBAC

-- Set database context for functions
USE DATABASE ${PREFIX}_database;
USE SCHEMA ${SCHEMA};

-- External function for Skyflow detokenization (PLAIN_TEXT)
-- Uses Python to make actual Skyflow API calls via external access integration
CREATE OR REPLACE FUNCTION ${PREFIX}_skyflow_detokenize(val VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = 3.12
HANDLER = 'detokenize_handler'
EXTERNAL_ACCESS_INTEGRATIONS = (${PREFIX}_SKYFLOW_EXTERNAL_ACCESS_INTEGRATION)
SECRETS = ('skyflow_pat_token' = SKYFLOW_PAT_TOKEN)
PACKAGES = ('requests')
AS
$$
import requests
import _snowflake

def detokenize_handler(val):
    """Detokenize a single token using Skyflow API."""
    
    # Handle null/empty values
    if not val or str(val).strip() == '':
        return val
    
    try:
        # Get PAT token from Snowflake secret
        pat_token = _snowflake.get_generic_secret_string('skyflow_pat_token')
        
        # Use substituted values for vault host and ID
        vault_host = "${SKYFLOW_VAULT_HOST}"
        vault_id = "${SKYFLOW_VAULT_ID}"
        
        # Construct Skyflow API URL using substituted values
        detokenize_url = f"https://{vault_host}/v1/vaults/{vault_id}/detokenize"
        
        # Prepare Skyflow detokenization payload (matching Databricks structure)
        payload = {
            "detokenizationParameters": [
                {
                    "token": str(val),
                    "redaction": "PLAIN_TEXT"
                }
            ]
        }
        
        # Set headers with Bearer token authentication
        headers = {
            "Authorization": f"Bearer {pat_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        # Make API call to Skyflow
        response = requests.post(detokenize_url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        
        # Parse response following Databricks pattern
        result = response.json()
        if (result and 'records' in result and len(result['records']) > 0 
            and 'value' in result['records'][0]):
            return result['records'][0]['value']
        else:
            # Fallback to original token if no value found
            return val
            
    except Exception as e:
        # On API failure, return the token (graceful degradation)
        return val
$$;

-- External function for Skyflow detokenization (MASKED/REDACTED)
CREATE OR REPLACE FUNCTION ${PREFIX}_skyflow_detokenize_masked(val VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON  
RUNTIME_VERSION = 3.12
HANDLER = 'detokenize_masked_handler'
EXTERNAL_ACCESS_INTEGRATIONS = (${PREFIX}_SKYFLOW_EXTERNAL_ACCESS_INTEGRATION)
SECRETS = ('skyflow_pat_token' = SKYFLOW_PAT_TOKEN)
PACKAGES = ('requests')
AS
$$
import requests
import _snowflake

def detokenize_masked_handler(val):
    """Detokenize and mask a token using Skyflow API."""
    
    # Handle null/empty values
    if not val or str(val).strip() == '':
        return val
    
    try:
        # Get PAT token from Snowflake secret
        pat_token = _snowflake.get_generic_secret_string('skyflow_pat_token')
        
        # Use substituted values for vault host and ID
        vault_host = "${SKYFLOW_VAULT_HOST}"
        vault_id = "${SKYFLOW_VAULT_ID}"
        
        # Construct Skyflow API URL using substituted values
        detokenize_url = f"https://{vault_host}/v1/vaults/{vault_id}/detokenize"
        
        # Prepare Skyflow detokenization payload with MASKED redaction
        payload = {
            "detokenizationParameters": [
                {
                    "token": str(val),
                    "redaction": "MASKED"
                }
            ]
        }
        
        # Set headers with Bearer token authentication
        headers = {
            "Authorization": f"Bearer {pat_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        # Make API call to Skyflow
        response = requests.post(detokenize_url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        
        # Parse response
        result = response.json()
        if (result and 'records' in result and len(result['records']) > 0 
            and 'value' in result['records'][0]):
            return result['records'][0]['value']
        else:
            # Fallback to original token if no value found
            return val
            
    except Exception as e:
        # On API failure, return the token (graceful degradation)
        return val
$$;

-- Multi-level conditional detokenization function with role-based redaction using Snowflake RBAC
-- Supports PLAIN_TEXT, MASKED, and token-only based on current role
CREATE OR REPLACE FUNCTION ${PREFIX}_skyflow_conditional_detokenize(val VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
  CASE
    -- Auditors get plain text (full detokenization)
    WHEN CURRENT_ROLE() = '${PREFIXED_PLAIN_TEXT_ROLE}' OR CURRENT_ROLE() = 'SYSADMIN' THEN 
      ${PREFIX}_skyflow_detokenize(val)
    -- Customer service gets masked data (partial redaction)
    WHEN CURRENT_ROLE() = '${PREFIXED_MASKED_ROLE}' THEN 
      ${PREFIX}_skyflow_detokenize_masked(val)
    -- Marketing and all other users get raw tokens (no API calls)
    ELSE val
  END
$$;

-- Convenience function for data masking policies (uses conditional logic)
CREATE OR REPLACE FUNCTION ${PREFIX}_skyflow_mask_detokenize(token VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
  ${PREFIX}_skyflow_conditional_detokenize(token)
$$;