-- Remove masking policies from PII columns

-- Set database context
USE DATABASE ${PREFIX}_database;
USE SCHEMA ${SCHEMA};

-- Remove masking policies from all PII columns
ALTER TABLE ${PREFIX}_customer_data MODIFY COLUMN first_name UNSET MASKING POLICY;
ALTER TABLE ${PREFIX}_customer_data MODIFY COLUMN last_name UNSET MASKING POLICY;
ALTER TABLE ${PREFIX}_customer_data MODIFY COLUMN email UNSET MASKING POLICY;
ALTER TABLE ${PREFIX}_customer_data MODIFY COLUMN phone_number UNSET MASKING POLICY;
ALTER TABLE ${PREFIX}_customer_data MODIFY COLUMN address UNSET MASKING POLICY;
ALTER TABLE ${PREFIX}_customer_data MODIFY COLUMN date_of_birth UNSET MASKING POLICY;