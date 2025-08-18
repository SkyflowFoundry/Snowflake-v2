-- Drop the sample customer data table

-- Set database context
USE DATABASE ${PREFIX}_database;
USE SCHEMA ${SCHEMA};

DROP TABLE IF EXISTS ${PREFIX}_customer_data;