-- Set database context
USE DATABASE ${PREFIX}_database;
USE SCHEMA ${SCHEMA};

CREATE TABLE IF NOT EXISTS ${PREFIX}_customer_data (
    customer_id VARCHAR(50) NOT NULL,
    -- PII columns that will be tokenized (data gets replaced with tokens)
    first_name VARCHAR(100),
    last_name VARCHAR(100), 
    email VARCHAR(255),
    phone_number VARCHAR(50),
    address VARCHAR(500),
    date_of_birth VARCHAR(50),  -- Changed to VARCHAR to store tokens
    -- Non-PII metadata columns
    signup_date TIMESTAMP,
    last_login TIMESTAMP,
    total_purchases NUMBER(10,0),
    total_spent NUMBER(10,2),
    loyalty_status VARCHAR(50),
    preferred_language VARCHAR(50),
    consent_marketing BOOLEAN,
    consent_data_sharing BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

INSERT INTO ${PREFIX}_customer_data (
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    address,
    date_of_birth,
    signup_date,
    last_login,
    total_purchases,
    total_spent,
    loyalty_status,
    preferred_language,
    consent_marketing,
    consent_data_sharing,
    created_at,
    updated_at
)
WITH numbered_rows AS (
  SELECT ROW_NUMBER() OVER (ORDER BY NULL) AS id
  FROM TABLE(GENERATOR(ROWCOUNT => 50))
),
base_data AS (
  SELECT
    id,
    CASE MOD(id - 1, 4)
      WHEN 0 THEN 'Jonathan'
      WHEN 1 THEN 'Jessica'
      WHEN 2 THEN 'Michael'
      WHEN 3 THEN 'Stephanie'
    END AS first_name,
    CASE MOD(id - 1, 4)
      WHEN 0 THEN 'Anderson'
      WHEN 1 THEN 'Williams'
      WHEN 2 THEN 'Johnson'
      WHEN 3 THEN 'Rodgers'
    END AS last_name,
    CASE MOD(id - 1, 10)
      WHEN 0 THEN 'London, England, SW1A 1AA'
      WHEN 1 THEN 'Paris, France, 75001'
      WHEN 2 THEN 'Berlin, Germany, 10115'
      WHEN 3 THEN 'Tokyo, Japan, 100-0001'
      WHEN 4 THEN 'Sydney, Australia, 2000'
      WHEN 5 THEN 'Toronto, Canada, M5H 2N2'
      WHEN 6 THEN 'Singapore, 238859'
      WHEN 7 THEN 'Dubai, UAE, 12345'
      WHEN 8 THEN 'São Paulo, Brazil, 01310-000'
      WHEN 9 THEN 'Mumbai, India, 400001'
    END AS city
  FROM numbered_rows
)
SELECT
  'CUST' || LPAD(id::STRING, 5, '0') AS customer_id,
  -- Real PII data that will be tokenized
  first_name AS first_name,
  last_name AS last_name,
  LOWER(first_name) || '.' || LOWER(last_name) || '@example.com' AS email,
  CASE MOD(id - 1, 10)
    WHEN 0 THEN '+1-555-0100'
    WHEN 1 THEN '+1-555-0101' 
    WHEN 2 THEN '+1-555-0102'
    WHEN 3 THEN '+1-555-0103'
    WHEN 4 THEN '+1-555-0104'
    WHEN 5 THEN '+1-555-0105'
    WHEN 6 THEN '+1-555-0106'
    WHEN 7 THEN '+1-555-0107'
    WHEN 8 THEN '+1-555-0108'
    WHEN 9 THEN '+1-555-0109'
  END AS phone_number,
  city AS address,
  CASE MOD(id - 1, 10)
    WHEN 0 THEN '1985-03-15'
    WHEN 1 THEN '1990-07-22'
    WHEN 2 THEN '1988-11-08'
    WHEN 3 THEN '1992-01-30'
    WHEN 4 THEN '1987-09-14'
    WHEN 5 THEN '1991-05-03'
    WHEN 6 THEN '1989-12-18'
    WHEN 7 THEN '1993-04-25'
    WHEN 8 THEN '1986-08-11'
    WHEN 9 THEN '1994-06-07'
  END AS date_of_birth,
  DATEADD('day', id - 1, '2018-01-01'::DATE) AS signup_date,
  DATEADD('day', id - 1, '2023-01-01'::DATE) AS last_login,
  id * 5 AS total_purchases,
  (id * 50.00)::NUMBER(10,2) AS total_spent,
  CASE MOD(id - 1, 4)
    WHEN 0 THEN 'Silver'
    WHEN 1 THEN 'Gold'
    WHEN 2 THEN 'Platinum'
    WHEN 3 THEN 'Diamond'
  END AS loyalty_status,
  CASE MOD(id - 1, 2) WHEN 0 THEN 'English' ELSE 'Spanish' END AS preferred_language,
  CASE MOD(id - 1, 2) WHEN 0 THEN TRUE ELSE FALSE END AS consent_marketing,
  CASE MOD(id - 1, 3) WHEN 0 THEN TRUE ELSE FALSE END AS consent_data_sharing,
  CURRENT_TIMESTAMP() AS created_at,
  CURRENT_TIMESTAMP() AS updated_at
FROM base_data;