        /* ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ 
        * принимает ('first_date' или 'last_date')
        * возвращент дату транзакции (самую раннюю или самую последнюю соответственно)
        * */
CREATE OR REPLACE FUNCTION fn_get_date(dates varchar)
	RETURNS SETOF date
	AS $$
		BEGIN
			IF (dates = 'first_date') THEN
				RETURN query
					SELECT transaction_datetime::date
					FROM Transactions
					ORDER BY 1
					LIMIT 1;
			ELSEIF (dates = 'last_date') THEN 
				RETURN query
					SELECT transaction_datetime::date
					FROM Transactions
					ORDER BY 1 DESC
					LIMIT 1;
			END IF;
		END;
$$ LANGUAGE plpgsql;
--проеврка функции:
--SELECT fn_get_date('first_date');
--SELECT fn_get_date('last_date');

CREATE OR REPLACE FUNCTION fn_check_measure_by_date(first_date date, last_date date, coefficient numeric)
	RETURNS TABLE (custumer_id integer, required_check_measure numeric)
	AS  $$
	BEGIN
		IF (first_date < fn_get_date('first_date'))
			THEN first_date = fn_get_date('first_date');
		ELSEIF (last_date > fn_get_date('last_date'))
			THEN last_date = fn_get_date('last_date');
		ELSEIF (first_date > last_date)
			THEN RAISE EXCEPTION 'начальная дата должна быть меньше последней';
		END IF;
	RETURN query
		WITH tmp AS (
			SELECT Cards.customer_id, Transactions.transaction_summ
			FROM Cards
			JOIN Transactions ON Cards.customer_card_id = transactions.customer_card_id
			WHERE Transactions.transaction_datetime BETWEEN first_date AND last_date
		)
		SELECT DISTINCT customer_id, (avg(transaction_summ) OVER (PARTITION BY customer_id)::NUMERIC * coefficient) AS required_check_measure
		FROM tmp;
	END;
$$ LANGUAGE plpgsql;

-- SELECT fn_check_measure_by_date('01-01-2017', '01-01-2023', 1)

CREATE OR REPLACE FUNCTION fn_check_measure_by_transaction(num_transactions integer, coefficient numeric)
	RETURNS TABLE (custumer_id integer, required_check_measure numeric)
	AS  $$
	BEGIN
	RETURN query
		WITH tmp AS (
			SELECT customer_card_id, transaction_summ
			FROM Transactions
			ORDER BY transaction_datetime DESC LIMIT num_transactions
		)
		SELECT Cards.customer_id, avg(transaction_summ) ::NUMERIC * coefficient AS required_check_measure
		FROM tmp
		JOIN Cards ON Cards.customer_card_id = tmp.customer_card_id
		GROUP BY Cards.customer_id
		ORDER BY 1;
	END;
$$ LANGUAGE plpgsql;

-- SELECT fn_check_measure_by_transaction(10, 1.0);