-- Active: 1692707583285@@127.0.0.1@5421@sql3_full
CREATE VIEW v_purchase_history AS 
SELECT Customer_ID, transactions.transaction_id, transaction_datetime, sku.Group_ID, 
sum(stores.SKU_Purchase_Price*checks.SKU_Amount) AS Group_Cost, sum(checks.sku_summ) AS Group_Summ, sum(checks.sku_summ_paid) AS Group_Summ_Paid    
FROM transactions
JOIN cards ON cards.customer_card_id = transactions.customer_card_id
JOIN checks ON checks.transaction_id = transactions.transaction_id
JOIN sku ON sku.sku_id = checks.sku_id
LEFT JOIN stores ON stores.transaction_store_id = transactions.transaction_store_id
WHERE stores.sku_id = checks.sku_id
GROUP BY Customer_ID, transactions.transaction_id, sku.Group_ID
ORDER BY Customer_ID, transactions.transaction_id, transaction_datetime, sku.Group_ID;

SELECT * FROM v_purchase_history;

DROP VIEW v_purchase_history;