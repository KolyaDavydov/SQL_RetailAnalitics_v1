-- Active: 1692707583285@@127.0.0.1@5421@sql3@public
-- Part 5. Формирование персональных предложений, ориентированных на рост частоты визитов
CREATE OR REPLACE FUNCTION fnc_personal_offers_freq_visits(first_date timestamp, last_date timestamp, transaction_num int, 
max_churn_idx int, max_share_trans_with_discount numeric, allow_margin_share numeric) 
RETURNS TABLE(Customer_ID int, Start_Date timestamp, End_Date timestamp, Required_Transactions_Count numeric, Group_Name varchar, Offer_Discount_Depth numeric)
AS $$ 
SELECT
    cm.customer_id,
    first_date, 
    last_date, 
    ROUND(EXTRACT(DAY FROM last_date-first_date)/cm.customer_frequency)+transaction_num,
    gs.group_name,
    dm.offer_discount_depth
FROM v_customers AS cm
JOIN fn_get_group(max_churn_idx,
                    max_share_trans_with_discount,
                    allow_margin_share) dm
ON cm.customer_id = dm.n_customer_id
JOIN groups_sku gs ON gs.group_id = dm.n_group_id
ORDER BY cm.customer_id;
$$ LANGUAGE sql;

-- Параметры функции:

-- первая и последняя даты периода
-- добавляемое число транзакций
-- максимальный индекс оттока
-- максимальная доля транзакций со скидкой (в процентах)
-- допустимая доля маржи (в процентах)

SELECT *
FROM fnc_personal_offers_freq_visits('2022-08-18', '2022-08-18', 1, 2, 50, 50);

SELECT *
FROM v_customers;

SELECT MAX(Group_Affinity_Index), Group_Margin FROM v_groups
WHERE v_groups.Customer_ID = Customer_ID AND Group_Churn_Rate <= max_churn_idx AND Group_Discount_Share < max_share_trans_with_discount
GROUP BY Group_Margin;