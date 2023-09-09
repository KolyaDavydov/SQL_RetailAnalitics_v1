-- БОЛЬШОЙ ДАТАСЕТ:
    -- время формирования представления  0,611 s
    -- строк - 57426
    -- отличная сходимость

-- МИНИ ДАТАСЕТ:
    -- время формирования представления  0,024 s
    -- строк - 200
    -- отличная сходимость


DROP MATERIALIZED VIEW IF EXISTS v_purchase_history CASCADE;
CREATE MATERIALIZED VIEW v_purchase_history AS 
SELECT
	Customer_ID													AS Customer_ID,
	transactions.transaction_id									AS Transaction_ID,
	transaction_datetime										AS Transaction_datetime,
	sku.Group_ID												AS Group_ID,
	sum(stores.SKU_Purchase_Price*checks.SKU_Amount)::numeric	AS Group_Cost,
	sum(checks.sku_summ)::numeric								AS Group_Summ,
	sum(checks.sku_summ_paid)::numeric							AS Group_Summ_Paid    
FROM transactions
JOIN cards ON cards.customer_card_id = transactions.customer_card_id
JOIN checks ON checks.transaction_id = transactions.transaction_id
JOIN sku ON sku.sku_id = checks.sku_id
LEFT JOIN stores ON stores.transaction_store_id = transactions.transaction_store_id
WHERE stores.sku_id = checks.sku_id
GROUP BY Customer_ID, transactions.transaction_id, sku.Group_ID
ORDER BY Customer_ID, transactions.transaction_id, transaction_datetime, sku.Group_ID;

-- для увеличения скорости обработки запросов
CREATE INDEX pur_idx ON v_purchase_history (customer_id, group_id, transaction_id, transaction_datetime);