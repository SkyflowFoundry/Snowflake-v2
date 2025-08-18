-- Create network rules and external access integration for Skyflow API access
-- This enables Snowflake to make outbound connections to Skyflow services

-- Set database context (network rules need database context)
USE DATABASE ${PREFIX_DATABASE};
USE SCHEMA ${SCHEMA};

-- Step 1: Create network rule to allow access to Skyflow API endpoints
CREATE OR REPLACE NETWORK RULE ${PREFIX}_SKYFLOW_APIS_NETWORK_RULE
 MODE = EGRESS
 TYPE = HOST_PORT
 VALUE_LIST = ('${SKYFLOW_VAULT_HOST}', 'manage.skyflowapis.com');

-- Step 2: Create external access integration linking network rules with secrets
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION ${PREFIX}_SKYFLOW_EXTERNAL_ACCESS_INTEGRATION
 ALLOWED_NETWORK_RULES = (${PREFIX}_SKYFLOW_APIS_NETWORK_RULE)
 ALLOWED_AUTHENTICATION_SECRETS = (${PREFIX_DATABASE}.PUBLIC.SKYFLOW_PAT_TOKEN)
 ENABLED = true;

-- Verify network rule was created
SHOW NETWORK RULES LIKE '${PREFIX}_SKYFLOW_APIS_NETWORK_RULE';

-- Verify external access integration was created  
SHOW EXTERNAL ACCESS INTEGRATIONS LIKE '${PREFIX}_SKYFLOW_EXTERNAL_ACCESS_INTEGRATION';

-- Verify secret exists and is accessible
SHOW SECRETS LIKE 'SKYFLOW_PAT_TOKEN' IN DATABASE ${PREFIX_DATABASE};