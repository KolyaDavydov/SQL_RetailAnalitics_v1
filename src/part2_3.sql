-- Active: 1692707583285@@127.0.0.1@5421@sql3_full
CREATE VIEW v_periods AS 
WITH cust AS (SELECT Customer_ID, sku.Group_ID, MIN(transactions.transaction_datetime) AS First_Group_Purchase_Date,
MAX(transactions.transaction_datetime) AS Last_Group_Purchase_Date,
COUNT(transactions.transaction_datetime) AS Group_Purchase,  SUM(checks.sku_summ_paid) AS Group_Summ_Paid    
FROM transactions
JOIN cards ON cards.customer_card_id = transactions.customer_card_id
JOIN checks ON checks.transaction_id = transactions.transaction_id
JOIN sku ON sku.sku_id = checks.sku_id
LEFT JOIN stores ON stores.transaction_store_id = transactions.transaction_store_id
WHERE stores.sku_id = checks.sku_id
GROUP BY Customer_ID, sku.Group_ID
ORDER BY Customer_ID, sku.Group_ID)
SELECT cust.Customer_ID, Group_ID, First_Group_Purchase_Date, Last_Group_Purchase_Date, Group_Purchase, 
EXTRACT(DAY FROM Last_Group_Purchase_Date-First_Group_Purchase_Date+'1 Day')/Group_Purchase AS Group_Frequency FROM cust;

SELECT * FROM v_periods;

DROP VIEW v_periods;