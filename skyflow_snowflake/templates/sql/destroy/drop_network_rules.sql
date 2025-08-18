-- Drop network rules and external access integration for Skyflow
-- Clean up network access configuration

-- Drop external access integration first (depends on network rules)
DROP EXTERNAL ACCESS INTEGRATION IF EXISTS ${PREFIX}_SKYFLOW_EXTERNAL_ACCESS_INTEGRATION;

-- Drop network rule
DROP NETWORK RULE IF EXISTS ${PREFIX}_SKYFLOW_APIS_NETWORK_RULE;

-- Verify cleanup
SHOW EXTERNAL ACCESS INTEGRATIONS LIKE '${PREFIX}_SKYFLOW_EXTERNAL_ACCESS_INTEGRATION';
SHOW NETWORK RULES LIKE '${PREFIX}_SKYFLOW_APIS_NETWORK_RULE';