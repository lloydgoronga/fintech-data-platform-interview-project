-- File: etl/create_dwh_schema.sql
-- This script defines the Star Schema for our Data Warehouse.

-- Drop tables if they exist to ensure a clean slate on each run.
DROP TABLE IF EXISTS fact_transactions CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_merchant CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

-- Dimension Table for Customers
-- Contains descriptive information about customers.
CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    first_name VARCHAR(50),
    last_name_hash VARCHAR(64), -- Hashed for PII protection
    email_hash VARCHAR(64)     -- Hashed for PII protection
);

-- Dimension Table for Merchants
-- Contains descriptive information about merchants.
CREATE TABLE dim_merchant (
    merchant_key SERIAL PRIMARY KEY,
    merchant_name VARCHAR(100) NOT NULL UNIQUE,
    category VARCHAR(50)
);

-- Dimension Table for Dates
-- Pre-populated with date attributes for easy time-based analysis.
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week SMALLINT,
    day_name VARCHAR(10),
    month SMALLINT,
    month_name VARCHAR(10),
    year SMALLINT
);

-- Fact Table for Transactions
-- Contains quantitative business metrics (the "facts").
-- It connects to dimensions via foreign keys.
CREATE TABLE fact_transactions (
    transaction_key SERIAL PRIMARY KEY,
    transaction_id INT,
    customer_key INT REFERENCES dim_customer(customer_key),
    merchant_key INT REFERENCES dim_merchant(merchant_key),
    date_key INT REFERENCES dim_date(date_key),
    amount DECIMAL(10, 2)
);

-- Create indexes on foreign keys in the fact table to dramatically speed up join performance.
CREATE INDEX idx_fact_customer_key ON fact_transactions(customer_key);
CREATE INDEX idx_fact_merchant_key ON fact_transactions(merchant_key);
CREATE INDEX idx_fact_date_key ON fact_transactions(date_key);