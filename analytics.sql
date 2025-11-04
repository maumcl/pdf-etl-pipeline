-- ===========================================================
-- 📘 Invoices Database Setup and Analytics
-- Author: Mauricio Lourenco
-- Description: Creates the `invoices` table and includes
--              key analytical queries for invoice insights.
-- ===========================================================


-- ===========================================================
-- 🏗️ 1. Table Definition
-- ===========================================================

CREATE TABLE IF NOT EXISTS invoices (
    id SERIAL PRIMARY KEY,
    company_id INT NOT NULL,
    country VARCHAR(50),
    processed_date DATE,
    supplier_name TEXT,
    supplier_tin VARCHAR(50),
    client_name TEXT,
    client_address TEXT,
    client_tin VARCHAR(50),
    invoice_number VARCHAR(50),
    invoice_date DATE,
    due_date DATE,
    description TEXT,
    qty FLOAT,
    price FLOAT,
    total FLOAT,
    tax_label FLOAT,
    tax_amount FLOAT,
    total_amount FLOAT,
    file TEXT
);


-- ===========================================================
-- 🧹 2. Maintenance Commands (Use with Caution)
-- ===========================================================

-- View all invoices
-- SELECT * FROM invoices;

-- Truncate and reset IDs
-- TRUNCATE TABLE invoices RESTART IDENTITY CASCADE;

-- Drop table if needed
-- DROP TABLE IF EXISTS invoices;


-- ===========================================================
-- 📊 3. Analytics Queries
-- ===========================================================


-- 🔹 3.1 Number of Documents Processed
SELECT 
    COUNT(DISTINCT invoice_number) AS quantity,
    invoice_number AS invoice
FROM invoices
GROUP BY invoice_number
ORDER BY invoice_number;


-- 🔹 3.2 Total Invoice/Receipt Value
SELECT 
    invoice_number AS invoice,
    SUM(DISTINCT total_amount) AS total_value
FROM invoices
GROUP BY invoice_number
ORDER BY total_value DESC;


-- 🔹 3.3 Top 5 Suppliers by Total Value
SELECT 
    supplier_name,
    SUM(total_amount) AS total_spent
FROM invoices
GROUP BY supplier_name
ORDER BY total_spent DESC
LIMIT 5;


-- 🔹 3.4 Most Common Products (Line Items)
SELECT 
    description,
    COUNT(*) AS occurrences
FROM invoices
GROUP BY description
ORDER BY occurrences DESC
LIMIT 5;


-- 🔹 3.5 Monthly Spending Trends
SELECT 
    TO_CHAR(DATE_TRUNC('month', invoice_date), 'Mon YYYY') AS month,
    SUM(total_amount) AS total_value
FROM invoices
GROUP BY TO_CHAR(DATE_TRUNC('month', invoice_date), 'Mon YYYY')
ORDER BY MIN(invoice_date);

