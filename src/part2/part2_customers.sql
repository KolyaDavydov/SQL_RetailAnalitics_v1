DROP TABLE IF EXISTS Customer_Segment;

CREATE TABLE Customer_Segment (
	segment INTEGER,
	avg_check VARCHAR,
	purchase_frequency VARCHAR,
	churn_probability VARCHAR
);

INSERT INTO Customer_Segment VALUES
	(1, 'Low', 'Rarely', 'Low'),
	(2, 'Low', 'Rarely', 'Medium'),
	(3, 'Low', 'Rarely', 'High'),
	(4, 'Low', 'Occasionally', 'Low'),
	(5, 'Low', 'Occasionally', 'Medium'),
	(6, 'Low', 'Occasionally', 'High'),
	(7, 'Low', 'Often', 'Low'),
	(8, 'Low', 'Often', 'Medium'),
	(9, 'Low', 'Often', 'High'),
	(10, 'Medium', 'Rarely', 'Low'),
	(11, 'Medium', 'Rarely', 'Medium'),
	(12, 'Medium', 'Rarely', 'High'),
	(13, 'Medium', 'Occasionally', 'Low'),
	(14, 'Medium', 'Occasionally', 'Medium'),
	(15, 'Medium', 'Occasionally', 'High'),
	(16, 'Medium', 'Often', 'Low'),
	(17, 'Medium', 'Often', 'Medium'),
	(18, 'Medium', 'Often', 'High'),
	(19, 'High', 'Rarely', 'Low'),
	(20, 'High', 'Rarely', 'Medium'),
	(21, 'High', 'Rarely', 'High'),
	(22, 'High', 'Occasionally', 'Low'),
	(23, 'High', 'Occasionally', 'Medium'),
	(24, 'High', 'Occasionally', 'High'),
	(25, 'High', 'Often', 'Low'),
	(26, 'High', 'Often', 'Medium'),
	(27, 'High', 'Often', 'High');
	
CREATE OR REPLACE FUNCTION customer_primary_store(IN in_customer_id INTEGER)
RETURNS INTEGER AS $$
DECLARE store_id INTEGER;
DECLARE recent_store_id INTEGER;
DECLARE no_of_most_recent_transactions_in_one_store INTEGER;
	BEGIN
		WITH count_store_visits AS (
			SELECT customer_id, transaction_store_id, COUNT(transaction_store_id) AS visits, MAX(transaction_datetime) AS most_recent
			FROM transactions t
			JOIN cards c ON t.customer_card_id = c.customer_card_id
			WHERE customer_id = in_customer_id
			GROUP BY 1, 2
		)
		SELECT transaction_store_id INTO store_id
		FROM (
			SELECT transaction_store_id, max_visits, MAX(most_recent)
			FROM (
				SELECT transaction_store_id, most_recent, MAX(visits) AS max_visits
				FROM count_store_visits
				GROUP BY 1, 2
				ORDER BY 3 DESC
			) a
			GROUP BY 1, 2
			ORDER BY 2 DESC, 3 DESC
			LIMIT 1
		) b;
		
		WITH latest_transactions AS (
			SELECT customer_id, transaction_store_id, transaction_datetime
			FROM transactions t
			JOIN cards c ON t.customer_card_id = c.customer_card_id
			WHERE customer_id = in_customer_id
			ORDER BY 1, 3 DESC
		)
		SELECT transaction_store_id INTO recent_store_id
		FROM (SELECT * FROM latest_transactions LIMIT 1) a;
		
		WITH latest_transactions AS (
			SELECT customer_id, transaction_store_id, transaction_datetime
			FROM transactions t
			JOIN cards c ON t.customer_card_id = c.customer_card_id
			WHERE customer_id = in_customer_id
			ORDER BY 1, 3 DESC
		)
		SELECT
			COUNT(*) INTO no_of_most_recent_transactions_in_one_store
		FROM (SELECT * FROM latest_transactions LIMIT 3) a
		JOIN (SELECT * FROM latest_transactions LIMIT 1) b
			ON a.transaction_store_id = b.transaction_store_id;

		IF no_of_most_recent_transactions_in_one_store >= 3 THEN
			RETURN recent_store_id;
		ELSE
			RETURN store_id;
		END IF;
	END;
$$ LANGUAGE plpgsql;


DROP MATERIALIZED VIEW IF EXISTS v_customers CASCADE;
CREATE MATERIALIZED VIEW v_customers AS
WITH
    total_summ AS (
        SELECT
            c.customer_id AS id,
            round(avg(t.transaction_summ), 2) AS avg_check
        FROM transactions AS t
        JOIN cards AS c
            ON t.customer_card_id = c.customer_card_id
        GROUP BY c.customer_id
        ORDER BY 1, 2
    ),
	customer_frequency AS (
		SELECT
            c.customer_id,
			ROUND(
				EXTRACT(epoch FROM (
					MAX(transaction_datetime) - MIN(transaction_datetime))
					/ NULLIF(COUNT(*), 0)
				) / (3600 * 24),
				2
			) AS avg_duration
        FROM transactions AS t
        JOIN cards AS c
            ON t.customer_card_id = c.customer_card_id
		GROUP BY customer_id
        ORDER BY 1
	)
SELECT
	customer_id,
	customer_average_check,
	customer_average_check_segment,
	customer_frequency,
	customer_frequency_segment,
	customer_inactive_period,
	customer_churn_rate,
	CASE
		WHEN Customer_Churn_Rate BETWEEN 0 AND 2 THEN 'Low'
		WHEN Customer_Churn_Rate BETWEEN 2 AND 5 THEN 'Medium'
		ELSE 'High'
	END AS Customer_Churn_Segment,
	cs.segment AS customer_segement,
	customer_primary_store(customer_id) AS customer_primary_store
FROM (
	SELECT
		id AS Customer_ID,
		avg_check AS Customer_Average_Check,
		CASE
			WHEN q."percent" >= 0.90 THEN 'High'
			WHEN q."percent" >= 0.65 THEN 'Medium'
			ELSE 'Low'
		END AS Customer_Average_Check_Segment,
		Customer_Frequency,
		CASE
			WHEN y."percent" >= 0.90 THEN 'Often'
			WHEN y."percent" >= 0.65 THEN 'Occasionally'
			ELSE 'Rarely'
		END AS Customer_Frequency_Segment,
		Customer_Inactive_Period,
		ROUND(Customer_Inactive_Period / Customer_Frequency, 2) AS Customer_Churn_Rate
	FROM (
		SELECT id, avg_check,
			ROUND(avg_check / max_check, 2) AS "percent"
		FROM total_summ
		JOIN (SELECT MAX(avg_check) AS max_check FROM total_summ) e
		ON 1 = 1
	) q
	JOIN (
		SELECT
			customer_id,
			avg_duration AS Customer_Frequency,
			ROUND(min_duration / avg_duration, 2) AS "percent"
		FROM customer_frequency
		JOIN (SELECT MIN(avg_duration) AS min_duration FROM customer_frequency) u
		ON 1 = 1
	) y ON y.customer_id = q.id
	JOIN (
		SELECT
			customer_id,
			ROUND(
				EXTRACT(epoch FROM (analysis_formation - MAX(transaction_datetime)) / (3600 * 24)),
				2
			) AS Customer_Inactive_Period
		FROM transactions t
		JOIN cards c ON t.customer_card_id = c.customer_card_id
		JOIN date_of_analysis_formation ON 1 = 1
		GROUP BY customer_id, analysis_formation
	) i ON i.customer_id = q.id
) o
JOIN Customer_Segment cs
	ON cs.avg_check = o.Customer_Average_Check_Segment
		AND cs.purchase_frequency = o.Customer_Frequency_Segment
		AND cs.churn_probability = (
			CASE
				WHEN Customer_Churn_Rate BETWEEN 0 AND 2 THEN 'Low'
				WHEN Customer_Churn_Rate BETWEEN 2 AND 5 THEN 'Medium'
				ELSE 'High'
			END
		)
ORDER BY 1;
		
SELECT * FROM v_customers;

