-- Verify Snowflake detokenization functions exist
-- Set database context first
USE DATABASE ${PREFIX}_database;
USE SCHEMA ${SCHEMA};

-- Check if functions exist
SHOW FUNCTIONS LIKE '${PREFIX}_skyflow_detokenize%';
SHOW FUNCTIONS LIKE '${PREFIX}_skyflow_conditional_detokenize';
SHOW FUNCTIONS LIKE '${PREFIX}_skyflow_mask_detokenize';

-- Check if masking policies exist
SHOW MASKING POLICIES LIKE '${PREFIX}_pii_mask';
SHOW MASKING POLICIES LIKE '${PREFIX}_date_mask';