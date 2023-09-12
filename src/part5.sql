-- Active: 1692707583285@@127.0.0.1@5421@sql3_full@public
-- Part 5. Формирование персональных предложений, ориентированных на рост частоты визитов
CREATE OR REPLACE FUNCTION fnc_personal_offers_freq_visits(first_date timestamp, last_date timestamp, transaction_num int, 
max_churn_idx int, max_share_trans_with_discount numeric, allow_margin_share numeric) 
RETURNS TABLE(Customer_ID int, Start_Date timestamp, End_Date timestamp, Required_Transactions_Count numeric, Group_Name varchar, Offer_Discount_Depth numeric)
AS $$ 
WITH grp AS (SELECT customer_id, MAX(Group_Affinity_Index), Group_Margin, group_margin*allow_margin_share AS Offer_Discount_Depth
FROM v_groups
WHERE v_groups.Customer_ID = Customer_ID AND Group_Churn_Rate <= max_churn_idx AND Group_Discount_Share < max_share_trans_with_discount
    AND abs(v_groups.group_margin  * allow_margin_share/100.) >= ceil((v_groups.group_minimum_discount*100.)/5.0)*0.05 * abs(v_groups.group_margin)
--    AND group_margin*allow_margin_share > group_minimum_discount
GROUP BY v_groups.customer_id, Group_Margin)
SELECT v_customers.customer_id, first_date, last_date, 
ROUND(EXTRACT(DAY FROM last_date-first_date)/customer_frequency)+transaction_num, customer_average_check_segment, 
group_margin*allow_margin_share
FROM v_customers
JOIN grp ON grp.customer_id = v_customers.customer_id
ORDER BY v_customers.customer_id;
$$ LANGUAGE sql;

-- Параметры функции:

-- первая и последняя даты периода
-- добавляемое число транзакций
-- максимальный индекс оттока
-- максимальная доля транзакций со скидкой (в процентах)
-- допустимая доля маржи (в процентах)

SELECT *
FROM fnc_personal_offers_freq_visits('2018-01-20', '2022-08-18', 10, 500, 0.5, 30);

SELECT *
FROM v_customers;

SELECT MAX(Group_Affinity_Index), Group_Margin FROM v_groups
WHERE v_groups.Customer_ID = Customer_ID AND Group_Churn_Rate <= max_churn_idx AND Group_Discount_Share < max_share_trans_with_discount
GROUP BY Group_Margin;