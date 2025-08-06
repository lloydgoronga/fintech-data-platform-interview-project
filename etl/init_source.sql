-- File: etl/init_source.sql
-- This script creates and seeds the transactional source database.
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100) UNIQUE,
    signup_date DATE
);
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    transaction_date TIMESTAMP,
    amount DECIMAL(10, 2),
    merchant_details VARCHAR(255) -- E.g., a simple string from a payment processor
);
INSERT INTO customers (customer_id, first_name, last_name, email, signup_date) VALUES
(1, 'Michael', 'Johnson', 'michael.j@example.com', '2023-01-15'),
(2, 'Sarah', 'Chen', 'sarah.c@example.com', '2023-02-20'),
(3, 'David', 'Rodriguez', 'david.r@example.com', '2023-03-10');
INSERT INTO transactions (customer_id, transaction_date, amount, merchant_details) VALUES
(1, '2025-08-01 10:00:00', 150.75, 'GreenLeaf Grocers'),
(2, '2025-08-01 11:30:00', 75.20, 'The Daily Grind Coffee'),
(1, '2025-08-02 14:00:00', 2500.00, 'TechSphere Electronics'),
(3, '2025-08-03 09:00:00', 45.50, 'GreenLeaf Grocers');