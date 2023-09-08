        /* ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ 
        * принимает ('first_date' или 'last_date')
        * возвращент дату транзакции (самую раннюю или самую последнюю соответственно)
        * */
DROP FUNCTION IF EXISTS fn_get_date(varchar);
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

        /* функция для определения условия приложения по крайним датам
         * МЕТОД 1 
        * принимает:
        * 		- диапазон дат('first_date' и 'last_date')
        * 		- коэффицикнт увеличения среднего чека
        * возвращент:
        * 		- идентификатор клиента
        * 		- целевое значение среднего чека
        * */
DROP FUNCTION IF EXISTS fn_check_measure_by_date(date, date, NUMERIC);
CREATE OR REPLACE FUNCTION fn_check_measure_by_date(first_date date, last_date date, coefficient NUMERIC)
	RETURNS TABLE (customer_id integer, required_check_measure NUMERIC)
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
		SELECT DISTINCT tmp.customer_id, (avg(transaction_summ) OVER (PARTITION BY tmp.customer_id)::NUMERIC * coefficient) AS required_check_measure
		FROM tmp;
	END;
$$ LANGUAGE plpgsql;
--проеврка функции:
-- SELECT fn_check_measure_by_date('01-01-2017', '01-01-2023', 1);

        /* функция для определения условия приложения по числу последних транзакций 
        * МЕТОД 2
        * принимает:
        * 		- число последних транзакций
        * 		- коэффицикнт увеличения среднего чека
        * возвращент:
        * 		- идентификатор клиента
        * 		- целевое значение среднего чека
        * */
DROP FUNCTION IF EXISTS fn_check_measure_by_transaction(integer, numeric);
CREATE OR REPLACE FUNCTION fn_check_measure_by_transaction(num_transactions integer, coefficient numeric)
	RETURNS TABLE (customer_id integer, required_check_measure numeric)
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

/* ОПРЕДЕЛЕНИЕ ГРУППЫ ДЛЯ ФОРМИРОВАНИЯ ВОЗНАГРАЖДЕНИЯ
 * Для формирования вознаграждения выбирается группа, отвечающая
последовательно следующим критериям:

		-Индекс востребованности группы – максимальный из всех возможных.
		
		
		-Индекс оттока по данной группе не должен превышать заданного пользователем значения. В случае, если коэффициент оттока превышает
		установленное значение, берется следующая по индексу
		востребованности группа;
		
		
		-Доля транзакций со скидкой по данной группе – менее заданного пользователем значения. В случае, если для выбранной группы превышает
		установленное значение, берется следующая по индексу
		востребованности группа, удовлетворяющая также критерию по
		оттоку.
		
		принимает:
			- g_churn_rate (максимальный индекс оттока),
					g_discount_share (максимальная доля транзакций со скидкой (в процентах)),
					g_margin (допустимая доля маржи (в процентах))
		возвращает
			- 'custumer_id'
			- 'group_id'
			- 'offer_discount_depth' удовлетворяющие условиям задачи
 * */
DROP FUNCTION IF EXISTS fn_get_group(NUMERIC, NUMERIC, NUMERIC);
CREATE OR REPLACE FUNCTION fn_get_group(
					g_churn_rate NUMERIC,
					g_discount_share NUMERIC,
					g_margin NUMERIC)
RETURNS TABLE (
	n_customer_id integer,
	n_group_id integer,
	offer_discount_depth NUMERIC)
AS $$
	BEGIN
		RETURN query (
			WITH tmp AS (SELECT
				v_groups.customer_id,
				max(v_groups.group_affinity_index) AS maxi
			FROM v_groups
			WHERE group_churn_rate <= g_churn_rate AND group_discount_share < g_discount_share / 100.
			GROUP BY customer_id
			ORDER BY customer_id)
			SELECT
				vg.customer_id AS customer_id,
				vg.group_id,
				ceil((vg.group_minimum_discount*100.)/5.0)*5.0
			--	vg.group_churn_rate,
			--	vg.group_discount_share,
			--	tmp.maxi
			FROM v_groups vg
			JOIN tmp ON vg.customer_id =tmp.customer_id AND vg.group_affinity_index = tmp.maxi
			WHERE vg.group_churn_rate <= g_churn_rate AND vg.group_discount_share < g_discount_share / 100.
			AND abs(vg.group_margin  * g_margin/100.) >= ceil((vg.group_minimum_discount*100.)/5.0)*0.05 * abs(vg.group_margin));
	END
$$ LANGUAGE plpgsql;
--для проверки функции (на мини датасете 5 строк)
--SELECT fn_get_group(1.15, 70.0, 30.0);


        /* ОСНОВНАЯ ФУНКЦИЯ 
        * Параметры функции:
			check_method 			- метод расчета среднего чека (1 - за период, 2 - за количество)
			first_date и last_date 	- первая и последняя даты периода (для 1 метода)
			num_transaction 		- количество транзакций (для 2 метода)
			coefficient 			- коэффициент увеличения среднего чека
			g_churn_rate 			- максимальный индекс оттока
			g_discount_share 		- максимальная доля транзакций со скидкой (в процентах)
			g_margin 				- допустимая доля маржи (в процентах)
        * возвращент необходимую таблицу
        * */
DROP FUNCTION IF EXISTS fn_main_part4(integer,
										date,
										date,
										integer,
										NUMERIC,
										NUMERIC,
										NUMERIC,
										NUMERIC);
CREATE OR REPLACE FUNCTION fn_main_part4(
										check_method integer,
										first_date date,
										last_date date,
										num_transaction integer,
										coefficient NUMERIC,
										g_churn_rate NUMERIC,
										g_discount_share NUMERIC,
										g_margin NUMERIC)
RETURNS TABLE (
	Customer_ID integer,
	Required_Check_Measure NUMERIC,
	Group_Name VARCHAR,
	Offer_Discount_Depth NUMERIC)
AS $$
BEGIN
	IF (check_method = 1) THEN
		RETURN query
			SELECT
				cm.customer_id,
				cm.required_check_measure,
				gs.group_name,
				dm.offer_discount_depth
			FROM fn_check_measure_by_date(first_date, last_date, coefficient) AS cm
			JOIN fn_get_group(g_churn_rate,
										g_discount_share,
										g_margin) dm
			ON cm.customer_id = dm.n_customer_id
			JOIN groups_sku gs ON gs.group_id = dm.n_group_id
			ORDER BY 1,2,4;
	ELSEIF (check_method = 2) THEN
		RETURN query
			SELECT
				cm.customer_id,
				cm.required_check_measure,
				gs.group_name,
				dm.offer_discount_depth
			FROM fn_check_measure_by_transaction(num_transaction, coefficient) AS cm
			JOIN fn_get_group(g_churn_rate,
										g_discount_share,
										g_margin) dm
			ON cm.customer_id = dm.n_customer_id
			JOIN groups_sku gs ON gs.group_id = dm.n_group_id
			ORDER BY 1,2,4;
	END IF;
END	
$$ LANGUAGE plpgsql;

-- расчет по количеству последних транзакций - метод 2
SELECT * FROM fn_main_part4(2, null, null,  100, 10, 3, 90, 30);
-- расчет по крайним датам - метод 1
SELECT * FROM fn_main_part4(1, '2021-09-02', '2023-01-01',  100, 10, 3, 70, 30);