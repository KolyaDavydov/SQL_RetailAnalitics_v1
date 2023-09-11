SELECT customer_id, transaction_store_id, transaction_datetime
FROM transactions t
JOIN cards c ON t.customer_card_id = c.customer_card_id
WHERE customer_id = 1
ORDER BY 1, 3 DESC;

WITH count_store_visits AS (
    SELECT customer_id, transaction_store_id, COUNT(transaction_store_id) AS visits, MAX(transaction_datetime) AS most_recent
    FROM transactions t
    JOIN cards c ON t.customer_card_id = c.customer_card_id
    WHERE customer_id = 4
    GROUP BY 1, 2
)
SELECT transaction_store_id, "max"
FROM (
    SELECT transaction_store_id, MAX(most_recent)
    FROM (
        SELECT transaction_store_id, most_recent, MAX(visits)
        FROM count_store_visits
        GROUP BY 1, 2
        ORDER BY 3 DESC
    ) a
    GROUP BY 1
    ORDER BY 1 DESC
) b;
