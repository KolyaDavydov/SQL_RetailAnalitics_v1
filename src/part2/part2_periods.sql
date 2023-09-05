--CREATE VIEW v_purchase_history AS 
--SELECT
--	Customer_ID										AS Customer_ID,
--	transactions.transaction_id						AS Trnsaction_ID,
--	transaction_datetime							AS Transaction_datetime,
--	sku.Group_ID									AS Group_ID,
--	sum(stores.SKU_Purchase_Price*checks.SKU_Amount) AS Group_Cost,
--	sum(checks.sku_summ)							AS Group_Summ,
--	sum(checks.sku_summ_paid)						AS Group_Summ_Paid    
--FROM transactions
--JOIN cards ON cards.customer_card_id = transactions.customer_card_id
--JOIN checks ON checks.transaction_id = transactions.transaction_id
--JOIN sku ON sku.sku_id = checks.sku_id
--LEFT JOIN stores ON stores.transaction_store_id = transactions.transaction_store_id
--WHERE stores.sku_id = checks.sku_id
--GROUP BY Customer_ID, transactions.transaction_id, sku.Group_ID
--ORDER BY Customer_ID, transactions.transaction_id, transaction_datetime, sku.Group_ID;
--
--
--CREATE VIEW v_periods AS 
--	WITH cust AS
--	(SELECT
--		Customer_ID								AS Customer_ID,
--		sku.Group_ID							AS Group_ID,
--		MIN(transactions.transaction_datetime)	AS First_Group_Purchase_Date,
--		MAX(transactions.transaction_datetime)	AS Last_Group_Purchase_Date,
--		COUNT(transactions.transaction_datetime) AS Group_Purchase,
--		SUM(checks.sku_summ_paid)				AS Group_Summ_Paid, 
--		MIN(checks.sku_discount/checks.sku_summ) AS Group_Min_Discount
--	FROM transactions
--	JOIN cards ON cards.customer_card_id = transactions.customer_card_id
--	JOIN checks ON checks.transaction_id = transactions.transaction_id
--	JOIN sku ON sku.sku_id = checks.sku_id
--	LEFT JOIN stores ON stores.transaction_store_id = transactions.transaction_store_id
--	WHERE stores.sku_id = checks.sku_id
--	GROUP BY Customer_ID, sku.Group_ID
--	ORDER BY Customer_ID, sku.Group_ID)
--SELECT
--	cust.Customer_ID,
--	Group_ID,
--	First_Group_Purchase_Date,
--	Last_Group_Purchase_Date,
--	Group_Purchase,
--	EXTRACT(DAY FROM Last_Group_Purchase_Date-First_Group_Purchase_Date+'1 Day')/Group_Purchase AS Group_Frequency,
--	Group_Min_Discount
--FROM cust;

CREATE VIEW v_groups AS
	WITH
		/* ИНДЕКС ВОСТРЕБОВАННОСТИ
		 *  1) Определяется общее количество транзакций клиента, совершенных им между первой и
			последней транзакциями с анализируемой группой (включая транзакции,
			в рамках которых не было анализируемой группы), включая первую и
			последнюю транзакции с группой. Для этого подсчитывается количество
			уникальных значений в поле Transaction_ID таблицы История покупок, дата совершения транзакций для которых больше или равна
			дате первой транзакции клиента с группой (значение поля
			First_Group_Purchase_Date таблицы Периоды) и меньше или
			равна дате последней транзакции клиента с группой (значение поля
			Last_Group_Purchase_Date таблицы Периоды)
			2) Количество транзакций с анализируемой группой
			(значение поля Group_Purchase таблицы Периоды) делится на общее количество транзакций клиента,
			совершенных с первой по последнюю транзакции, в которых была
			анализируемая группа. Итоговое значение
			сохраняется для группы в поле Group_Affinity_Index таблицы Группы.
		 * 
		 * 1) определяем общее количество транзакций клиента в период 'i_1'
		 * 2) индекс востребованности = количество транзакций с группой / общее количество транзакций клиента 'i_2'
		 * */
		g_affinity_index AS (
			SELECT
				vph.customer_id,
				vp.group_id,
				vp.group_purchase/count(vph.trnsaction_id)::NUMERIC AS group_affinity_index -- i_2
			FROM v_purchase_history vph
			JOIN v_periods vp ON vp.customer_id = vph.customer_id 
			WHERE vph.transaction_datetime  BETWEEN vp.first_group_purchase_date AND vp.last_group_purchase_date --i_1
			GROUP BY vph.customer_id, vp.group_id, vp.group_purchase
			ORDER BY 1,2),
		/* ИНДЕКС ОТТОКА ИЗ ГРУППЫ
		 1) Из даты формирования анализа вычитается
		дата последней транзакции клиента, в которой была представлена
		анализируемая группа. Для определения последней даты покупки группы
		клиентом выбирается максимальное значение по полю Transaction_DateTime
		таблицы История покупок для записей, в которых значения полей
		Customer_ID и Group_ID соответствуют значениям аналогичных полей
		таблицы Группы.
		2) Количество дней, прошедших после
		даты последней транзакции клиента с анализируемой группой, делится на среднее количество дней между покупками
		анализируемой группы клиентом (значение поля Group_Frequency
		таблицы Периоды). Итоговое значение сохраняется в поле
		Group_Churn_Rate таблицы Группы.
		 * = (дата формирования анализа - дата последней транзакции) / на среднее количество дней между покупками
		 * */
		g_churn_rate AS (
			SELECT
				vph.customer_id,
				vph.group_id,
				(EXTRACT(epoch FROM(SELECT * FROM date_of_analysis_formation)) - EXTRACT(epoch FROM max(vph.transaction_datetime)))/(vp.group_frequency)/(60*60*24)::NUMERIC AS Group_Churn_Rate
			FROM transactions t
			JOIN v_purchase_history vph ON t.transaction_id = vph.trnsaction_id
			JOIN v_periods vp ON vph.customer_id=vp.customer_id AND vph.group_id=vp.group_id
			GROUP BY vph.customer_id, vph.group_id, vp.group_frequency),
		/* ИНДЕКС СТАБИЛЬНОСТИ ПОТРЕБЛЕНИЯ ГРУППЫ
		1) Расчет интервалов потребления группы. Определяются все интервалы
		(в количестве дней) между транзакциями клиента, содержащими
		анализируемую группу. Для этого все транзакции, содержащие
		анализируемую группу в покупках клиента, ранжируются по дате
		совершения (значению поля Transaction_DateTime таблицы История покупок) от самой ранней к самой поздней. Из даты каждой
		последующей транзакции вычитается дата предыдущей. Каждый интервал
		учитывается отдельно. (помогает функция LAG, которая возвращает предыдущую строку)
		
		2) Подсчет абсолютного отклонения каждого интервала от средней
		частоты покупок группы. Из значения каждого интервала вычитается
		среднее количество дней между транзакциями с анализируемой группой
		(значение поля Group_Frequency таблицы Периоды). В случае,
		если получившееся значение является отрицательным, оно умножается на
		-1. (можно сделать модуль)
		
		3) Подсчет относительного отклонения каждого интервала от средней
		частоты покупок группы. Получившееся на предыдущем шаге значение для
		каждого интервала делится на среднее количество дней между
		транзакциями с анализируемой группой (значение поля
		Group_Frequency таблицы Периоды).
		
		
		4) Определение стабильности потребления группы. Показатель
		стабильности потребления группы определяется как среднее значение
		всех показателей, получившихся на предыдущем шаге. Результат сохраняется в
		поле Group_Stability_Index таблицы Группы.
		 * */
		g_stability AS (
			SELECT
				vp.customer_id,
				vp.group_id,
				ABS(EXTRACT(epoch FROM t.transaction_datetime - LAG(t.transaction_datetime, 1) OVER (PARTITION BY vp.customer_id, vp.group_id ORDER BY t.transaction_datetime))::float /
                              86400.0 - vp.group_frequency) / vp.group_frequency AS Group_Stability_Index_Not_Avg
			FROM transactions t
			JOIN v_purchase_history vph ON t.transaction_id = vph.trnsaction_id
			JOIN v_periods vp ON vph.customer_id=vp.customer_id AND vph.group_id=vp.group_id
			GROUP BY vp.customer_id, vp.group_id, t.transaction_datetime, vp.group_frequency),
			
		g_stability_index AS (
			SELECT
				customer_id,
				group_id,
				avg(Group_Stability_Index_Not_Avg) AS Group_Stability_Index
			FROM g_stability
			GROUP BY customer_id, group_id),
			
		/* АКТУАЛЬНАЯ МАРЖА ПО ГРУППЕ
		 * 
		 * */
		g_margin AS (
			SELECT
				vph.customer_id,
				vph.group_id,
				sum(vph.group_summ_paid-vph.group_cost)::NUMERIC AS Group_Margin
			FROM v_purchase_history vph
			GROUP BY vph.customer_id, vph.group_id)
	SELECT
		gax.customer_id,
		gax.group_id,
		gax.group_affinity_index,
		gcr.group_churn_rate,
		gsi.group_stability_index,
		gm.group_margin
	FROM g_affinity_index gax
	JOIN g_churn_rate gcr ON gcr.customer_id = gax.customer_id AND gcr.group_id = gax.group_id
	JOIN g_stability_index gsi ON gsi.customer_id = gax.customer_id AND gsi.group_id = gax.group_id
	JOIN g_margin gm ON gm.customer_id = gax.customer_id AND gm.group_id = gax.group_id;
	
SELECT * FROM v_groups;
DROP VIEW v_groups;