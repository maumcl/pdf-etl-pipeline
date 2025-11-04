📈 Next Steps — Data Analysis

Once the data is in PostgreSQL, you can perform the following analyses:

🔹 Number of Documents Processed
SELECT COUNT(DISTINCT invoice_number) AS num_invoices
FROM invoices;

🔹 Total Invoice/Receipt Value
SELECT SUM(total_amount) AS total_value
FROM invoices;

🔹 Top 5 Suppliers by Total Value
SELECT supplier_name, SUM(total_amount) AS total_spent
FROM invoices
GROUP BY supplier_name
ORDER BY total_spent DESC
LIMIT 5;

🔹 Most Common Products (Line Items)
SELECT description, COUNT(*) AS occurrences
FROM invoices
GROUP BY description
ORDER BY occurrences DESC
LIMIT 5;

🔹 Monthly Spending Trends
SELECT DATE_TRUNC('month', invoice_date) AS month, SUM(total_amount) AS total_value
FROM invoices
GROUP BY month
ORDER BY month;