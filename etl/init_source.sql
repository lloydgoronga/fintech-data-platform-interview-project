-- This script creates and seeds the transactional source database with an expanded dataset.

-- Drop existing tables to ensure a clean start on re-initialization
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS customers;

-- Create a simple customers table
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100) UNIQUE,
    signup_date DATE
);

-- Create a transactions table
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    transaction_date TIMESTAMP,
    amount DECIMAL(10, 2),
    merchant_details VARCHAR(255)
);

-- Insert an expanded list of customers
INSERT INTO customers (customer_id, first_name, last_name, email, signup_date) VALUES
(1, 'Michael', 'Johnson', 'michael.j@example.com', '2023-01-15'),
(2, 'Sarah', 'Chen', 'sarah.c@example.com', '2023-02-20'),
(3, 'David', 'Rodriguez', 'david.r@example.com', '2023-03-10'),
(4, 'Emily', 'White', 'emily.w@example.com', '2023-04-05'),
(5, 'Chris', 'Green', 'chris.g@example.com', '2023-05-21');

-- Insert a larger and more varied set of transactions over a full week
INSERT INTO transactions (customer_id, transaction_date, amount, merchant_details) VALUES
-- Day 1
(1, '2025-08-01 10:00:00', 150.75, 'GreenLeaf Grocers'),
(2, '2025-08-01 11:30:00', 75.20, 'The Daily Grind Coffee'),
(4, '2025-08-01 15:00:00', 12.50, 'The Daily Grind Coffee'),
(5, '2025-08-01 18:45:00', 350.00, 'Fine Dine Restaurant'),
-- Day 2
(3, '2025-08-02 09:15:00', 88.40, 'GreenLeaf Grocers'),
(1, '2025-08-02 14:00:00', 2500.00, 'TechSphere Electronics'),
(2, '2025-08-02 16:20:00', 25.00, 'City Cinema'),
-- Day 3
(5, '2025-08-03 11:00:00', 45.50, 'GreenLeaf Grocers'),
(4, '2025-08-03 13:10:00', 120.00, 'GlobalMart'),
(1, '2025-08-03 19:30:00', 55.60, 'Fine Dine Restaurant'),
-- Day 4
(2, '2025-08-04 08:30:00', 8.90, 'The Daily Grind Coffee'),
(3, '2025-08-04 12:00:00', 220.00, 'GlobalMart'),
-- Day 5
(1, '2025-08-05 17:00:00', 4500.00, 'Luxury Watches Inc.'),
(5, '2025-08-05 18:00:00', 62.00, 'GreenLeaf Grocers'),
(4, '2025-08-05 20:00:00', 35.00, 'City Cinema'),
-- Day 6
(2, '2025-08-06 10:45:00', 95.00, 'GlobalMart'),
(3, '2025-08-06 13:00:00', 1800.00, 'TechSphere Electronics'),
-- Day 7
(1, '2025-08-07 09:00:00', 15.00, 'The Daily Grind Coffee'),
(4, '2025-08-07 16:50:00', 78.50, 'Fine Dine Restaurant'),
(5, '2025-08-07 21:00:00', 3100.00, 'Luxury Watches Inc.');
