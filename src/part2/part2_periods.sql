-- БОЛЬШОЙ ДАТАСЕТ:
    -- время формирования представления  0,508 s
    -- строк - 25245
    -- отличная сходимость

-- МАЛЫЙ ДАТАСЕТ:
    -- время формирования представления  0,031 s
    -- строк - 55
    -- отличная сходимость

DROP MATERIALIZED VIEW IF EXISTS v_periods CASCADE;
CREATE MATERIALIZED VIEW v_periods AS 
	WITH
		cust AS (
			SELECT  pd.customer_id, group_id, t.transaction_id, (sku_discount/sku_summ)::numeric as Group_Min_Discount
			FROM person_data pd
			JOIN cards c ON pd.customer_id = c.customer_id
			JOIN transactions t ON c.customer_card_id = t.customer_card_id
			JOIN checks c2 ON c2.transaction_id = t.transaction_id
			JOIN sku s on c2.sku_id = s.sku_id
			GROUP BY pd.customer_id, s.group_id, t.transaction_id, (sku_discount/sku_summ)::numeric
			ORDER BY pd.customer_id ),
		Date_First_Last_Purchase AS (
			SELECT vph.customer_id, vph.group_id, min((transaction_datetime)) AS First_Group_Purchase_Date, max((transaction_datetime)) AS Last_Group_Purchase_Date,
				count(vph.transaction_id) AS Group_Purchase
			FROM v_purchase_history vph
			GROUP BY vph.customer_id, vph.group_id
			ORDER BY vph.customer_id, group_id ),
		Frequency_Purchase AS (
			SELECT d.customer_id, d.group_id, ((extract(epoch from Last_Group_Purchase_Date -  First_Group_Purchase_Date)/86400 + 1) / Group_Purchase)::numeric AS Group_Frequency
			FROM Date_First_Last_Purchase d
        )
		SELECT 
			D.Customer_ID,
			D.Group_ID,
			First_Group_Purchase_Date,
			Last_Group_Purchase_Date,
			Group_Purchase,
			Group_Frequency,
			CASE
				WHEN max(group_min_discount) = 0 THEN 0
				ELSE (min(Group_Min_Discount) FILTER ( WHERE group_min_discount > 0 ))
			END AS Group_Min_Discount
		FROM cust P
		JOIN Date_First_Last_Purchase D ON D.customer_id = P.customer_id AND p.group_id = d.group_id
		JOIN Frequency_Purchase F ON F.customer_id = D.customer_id AND f.group_id = p.group_id
		GROUP BY D.group_id, d.customer_id, First_Group_Purchase_Date, Last_Group_Purchase_Date, Group_Purchase, Group_Frequency
		ORDER BY D.customer_id, D.group_id;

CREATE INDEX per_idx ON v_periods (customer_id, group_id, first_group_purchase_date, last_group_purchase_date,
                                 group_purchase, group_frequency, group_min_discount);