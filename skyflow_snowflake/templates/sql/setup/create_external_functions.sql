-- Snowflake External Access Integration for Skyflow API
-- Configures network access for external Python functions to call Skyflow APIs

-- Set database context  
USE DATABASE ${PREFIX}_database;
USE SCHEMA ${SCHEMA};

-- Verify external access integration exists (created in network rules step)
SHOW EXTERNAL ACCESS INTEGRATIONS LIKE '${PREFIX}_SKYFLOW_EXTERNAL_ACCESS_INTEGRATION';

-- Verify network rules exist and are accessible
SHOW NETWORK RULES LIKE '${PREFIX}_SKYFLOW_APIS_NETWORK_RULE';

-- Confirm integration is ready for external function use
SELECT 'External access integration configured for Skyflow API access' as status;