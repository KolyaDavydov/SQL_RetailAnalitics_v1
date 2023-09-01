WITH
    total_summ AS (
        SELECT
            c.customer_id,
            round(avg(t.transaction_summ), 2) AS customer_average_check
        FROM transactions AS t
        JOIN cards AS c
            ON t.customer_card_id = c.customer_card_id
        GROUP BY c.customer_id
        ORDER BY 1, 2
    ),
    max_avg_check AS (
        SELECT max(customer_average_check)
        FROM total_summ;
    )
SELECT
    customer_id,
    customer_average_check
FROM total_summ;

