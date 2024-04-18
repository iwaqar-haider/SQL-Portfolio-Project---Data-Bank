--1.
select distinct(node_id) from customer_nodes
-- 2.
select reg.region_name, inn.TotalNodes from regions reg
join (
select region_id,count(node_id) as TotalNodes from customer_nodes
group by region_id) 
inn
on reg.region_id = inn.region_id
-- 3.
select reg.region_name, inn.TotalCustomer from regions reg
join (
select region_id, count(customer_id) as TotalCustomer from customer_nodes
group by region_id )
inn
on reg.region_id = inn.region_id

--4.
SELECT AVG(DATEDIFF(day, start_date, end_date)) AS avg_reallocation_days
FROM customer_nodes
WHERE end_date != '9999-12-31'
--5.

WITH cte AS (
  SELECT
    region_id,
    DATEDIFF(day, start_date, end_date) AS reallocation_days
  FROM customer_nodes
  WHERE end_date != '9999-12-31'
)
SELECT
  region_id,
  PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY reallocation_days) OVER (PARTITION BY region_id) AS median,
  PERCENTILE_DISC(0.8) WITHIN GROUP (ORDER BY reallocation_days) OVER (PARTITION BY region_id) AS p80th,
  PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY reallocation_days) OVER (PARTITION BY region_id) AS p95th
FROM cte;


-- 1. Unique Count and Total Amount per Transaction Type:
SELECT txn_type,
       COUNT(DISTINCT customer_id) AS unique_customers,
       SUM(txn_amount) AS total_amount
FROM customer_transactions
GROUP BY txn_type;

-- 2. Average Historical Deposit Counts and Amounts:
SELECT 
  AVG(CASE WHEN txn_type = 'Deposit' THEN COUNT(*) ELSE 0 END) AS avg_deposit_count,
  AVG(CASE WHEN txn_type = 'Deposit' THEN SUM(txn_amount) ELSE 0 END) AS avg_deposit_amount
FROM customer_transactions;

-- 3. Data Bank Customers with Multiple Transactions:
SELECT YEAR(txn_date) AS year,
       MONTH(txn_date) AS month,
       COUNT(DISTINCT customer_id) AS multi_transaction_customers
FROM customer_transactions
WHERE customer_id IN (
  SELECT customer_id
  FROM customer_transactions
  GROUP BY customer_id
  HAVING COUNT(DISTINCT txn_type) >= 2 AND (COUNT(txn_type) = 2 OR txn_type IN ('Deposit', 'Purchase', 'Withdrawal'))
)
GROUP BY YEAR(txn_date), MONTH(txn_date)
HAVING customer_id IS NOT NULL;


-- 4. Closing Balance per Customer:
WITH cte AS (
  SELECT customer_id,
         txn_date,
         txn_amount,
         SUM(txn_amount) OVER (PARTITION BY customer_id ORDER BY txn_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
  FROM customer_transactions
)
SELECT customer_id, txn_date, running_total AS closing_balance
FROM cte
WHERE DAY(txn_date) = (SELECT MAX(DAY(txn_date)) FROM customer_transactions);

-- 5. Percentage of Customers with Increased Closing Balance:
WITH cte AS (
  SELECT customer_id,
         txn_date,
         txn_amount,
         SUM(txn_amount) OVER (PARTITION BY customer_id ORDER BY txn_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
  FROM customer_transactions
)
SELECT 
  COUNT(*) AS total_customers,
  COUNT(DISTINCT customer_id) AS improved_customers,
  ROUND((COUNT(DISTINCT customer_id) / COUNT(*) * 100), 2) AS improvement_percentage
FROM (
  SELECT customer_id, running_total,
         LAG(running_total) OVER (PARTITION BY customer_id ORDER BY txn_date) AS prev_running_total
  FROM cte
  WHERE DAY(txn_date) = (SELECT MAX(DAY(txn_date)) FROM customer_transactions)
) AS last_day_balances
WHERE prev_running_total IS NOT NULL AND running_total - prev_running_total > 0.05 * prev_running_total;


-- Data provisioning estimate for data bank experiment
-- We can use CTE to calculate running balance of customers
WITH cte AS (
  SELECT customer_id,
         txn_date,
         txn_amount,
         SUM(txn_amount) OVER (PARTITION BY customer_id ORDER BY txn_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
  FROM customer_transactions
)
--
--Option 1: End-of-Month Balance

--Data Required:

--Monthly data would be sufficient.
--You can extract the running_total for each customer at the end of each month (using functions like MAX(DAY(txn_date)) or similar logic).
--Option 2: Average Balance (Previous 30 Days)

--Data Required:

--This requires daily data for the previous 30 days for each month.
--You can use window functions like AVG(running_total) OVER (PARTITION BY customer_id ORDER BY txn_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW).
--This calculates the average running balance for the past 30 days for each customer on a daily basis.
--Option 3: Real-time Balance

--Data Required:

--This requires the most data, as it needs the latest running balance for every transaction.
--The entire customer_transactions table with the running_total calculated in the CTE would be needed.
--Estimating Data Volume:

--The amount of data needed depends on your table structure and the number of customers. However, here's a general breakdown:

--Option 1: This requires the least data (monthly snapshots of running balance).
--Option 2: This requires more data than Option 1 but less than Option 3 (daily calculations for the past 30 days).
--Option 3: This needs the most data (all transactions with running balance).

-- Extra Challenge:

--Here's how to estimate data provisioning needs for Data Bank's option with daily interest (without compounding):

--Data Required:

--This approach requires daily data for each customer.
--You need the following for each day:
--Customer ID
--Transaction date (txn_date)
--Transaction amount (txn_amount)
--Previous day's running balance (requires additional calculation)
--Calculating Daily Interest:

--Calculate the daily interest rate: Divide the annual interest rate (6%) by the number of days in a year (365 or 366 depending on leap year).
--Use the previous day's running balance to calculate the daily interest amount for each customer.
--Estimating Data Volume:

--Compared to the previous options, this approach needs more data due to daily calculations.
--You'll need to store:
--Original transaction data (customer_id, txn_date, txn_amount)
--Daily running balance for each customer
--Impact on Monthly Data:

--While calculations happen daily, data provisioning can still be estimated on a monthly basis.
--You can calculate the total data used for each month by summing the daily data size.
--Compounding Interest (Optional):

--Calculating daily compounding interest requires a more complex approach. Here's a general outline:

--Instead of using the previous day's balance, use the current day's running balance for the next day's interest calculation.
--This creates a compounding effect where interest is earned on previously accrued interest.
--The data requirements would be similar to the non-compounding case, but calculations would be more complex.
--Important Considerations:

--This is a simplified estimation, and the actual data volume may vary depending on factors like data types and storage format.
--Implementing compounding interest adds complexity and potentially increases storage needs.
--By analyzing the estimated data volume for this option, the Data Bank team can weigh it against the benefits of interest-based data allocation before implementing it.