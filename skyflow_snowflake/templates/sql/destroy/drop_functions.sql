-- Drop Snowflake functions and policies created for Skyflow integration

-- Set database context
USE DATABASE ${PREFIX}_database;
USE SCHEMA ${SCHEMA};

-- Drop the functions (current signatures only)
DROP FUNCTION IF EXISTS ${PREFIX}_skyflow_detokenize(VARCHAR, VARCHAR, VARCHAR);
DROP FUNCTION IF EXISTS ${PREFIX}_skyflow_conditional_detokenize(VARCHAR);
DROP FUNCTION IF EXISTS ${PREFIX}_skyflow_mask_detokenize(VARCHAR);

-- Drop masking policies
DROP MASKING POLICY IF EXISTS ${PREFIX}_pii_mask;
DROP MASKING POLICY IF EXISTS ${PREFIX}_date_mask;

-- Drop API integration
DROP API INTEGRATION IF EXISTS SKYFLOW_API_INTEGRATION;