-- Apply Snowflake Data Masking Policies to PII columns for role-based access
-- Snowflake RBAC with masking policies for role-based data access
-- Role-based access: AUDITOR sees detokenized, CUSTOMER_SERVICE sees masked, others see tokens

-- Set database context
USE DATABASE ${PREFIX}_database;
USE SCHEMA ${SCHEMA};

-- Create masking policy for PII data with role-based logic
-- After tokenization, ALL PII columns contain tokens (any format)
-- This policy detokenizes based on user role ONLY (no token format checking)
CREATE OR REPLACE MASKING POLICY ${PREFIX}_pii_mask AS (val VARCHAR) RETURNS VARCHAR ->
  CASE 
    WHEN CURRENT_ROLE() = '${PREFIXED_PLAIN_TEXT_ROLE}' OR CURRENT_ROLE() = 'SYSADMIN' THEN
      -- Auditors always see detokenized data (calls Skyflow API or mock)
      ${PREFIX}_skyflow_detokenize(val)
    WHEN CURRENT_ROLE() = '${PREFIXED_MASKED_ROLE}' THEN
      -- Customer service gets masked/redacted version
      ${PREFIX}_skyflow_detokenize_masked(val)
    ELSE 
      -- Marketing and other roles see raw tokens (no detokenization)
      val
  END;

-- Apply masking policy to PII columns in the customer data table
-- Note: In a real implementation, you'd apply this to both the original PII columns 
-- and the token columns, depending on your tokenization strategy

-- Apply masking policy to all PII columns (now containing tokens)
ALTER TABLE ${PREFIX}_customer_data MODIFY COLUMN first_name SET MASKING POLICY ${PREFIX}_pii_mask;
ALTER TABLE ${PREFIX}_customer_data MODIFY COLUMN last_name SET MASKING POLICY ${PREFIX}_pii_mask;
ALTER TABLE ${PREFIX}_customer_data MODIFY COLUMN email SET MASKING POLICY ${PREFIX}_pii_mask;
ALTER TABLE ${PREFIX}_customer_data MODIFY COLUMN phone_number SET MASKING POLICY ${PREFIX}_pii_mask;
ALTER TABLE ${PREFIX}_customer_data MODIFY COLUMN address SET MASKING POLICY ${PREFIX}_pii_mask;
ALTER TABLE ${PREFIX}_customer_data MODIFY COLUMN date_of_birth SET MASKING POLICY ${PREFIX}_pii_mask;

-- Date of birth is now VARCHAR (tokenized), so use same PII policy
-- No separate date policy needed since it's tokenized like other PII