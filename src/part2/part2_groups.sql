--!!!! Без возможности выбора метода для margin

-- большой датасет:
    -- время формирования представления  2,181 s
    -- строк - 25245
    -- сходимость хорошая. Не сходятся 
    --                             - group_affinity_index

-- МАЛЫЙ ДАТАСЕТ:
    -- время формирования представления  0,081 s
    -- строк - 55
    -- сходимость отличная 
	
DROP MATERIALIZED VIEW IF EXISTS v_groups CASCADE;
CREATE MATERIALIZED VIEW v_groups AS
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
				COALESCE(avg(Group_Stability_Index_Not_Avg), 1) AS Group_Stability_Index
			FROM g_stability
			GROUP BY customer_id, group_id),
			
		/* АКТУАЛЬНАЯ МАРЖА ПО ГРУППЕ
		 * !!! добавить возможность выбора расчета!!!
		 * */
		g_margin AS (
			SELECT
				vph.customer_id,
				vph.group_id,
				sum(vph.group_summ_paid-vph.group_cost)::NUMERIC AS Group_Margin
			FROM v_purchase_history vph
			GROUP BY vph.customer_id, vph.group_id),
			
		/* ОПРЕДЕЛЕНИЕ КОЛИЧЕСТВА ТРАНЗАКЦИЙ КЛИЕНТА СО СКИДКОЙ.
		 * Определяется количество транзакций, в рамках которых анализируемая
		группа была приобретена клиентом с применением какой-либо скидки.
		Для подсчета используются уникальные значения по полю
		Transaction_ID таблицы Чеки для транзакций, в рамках которых
		клиент приобретал анализируемую группу, при этом значение поля
		SKU_Discount таблицы Чеки больше нуля. Скидка,
		представленная в рамках списания бонусных баллов, не учитывается.
		 * */
		g_amount_transaction AS (
			SELECT DISTINCT
				pd.customer_id,
				s.group_id,
				count(c2.transaction_id) FILTER (WHERE c2.sku_discount > 0) AS count_discont
			FROM person_data pd
			JOIN cards c ON pd.customer_id=c.customer_id
			JOIN transactions t ON c.customer_card_id=t.customer_card_id
			JOIN checks c2 ON t.transaction_id=c2.transaction_id
			JOIN sku s ON s.sku_id=c2.sku_id
			GROUP BY pd.customer_id, s.group_id),
		/* ООПРЕДЕЛЕНИЕ ДОЛИ ТРАНЗАКЦИЙ СО СКИДКОЙ
		 * Количество транзакций, в
		рамках которых приобретение товаров из анализируемой группы было
		совершено со скидкой делится на общее
		количество транзакций клиента с анализируемой группой за
		анализируемый период (данные поля Group_Purchase таблицы Периоды для анализируемой группы по клиенту). Получившееся значения
		сохраняется в качестве доли транзакций по покупке анализируемой
		группы со скидкой в поле Group_Discount_Share таблицы Группы.
		 * */
		g_discount_share AS (
			SELECT
				gat.customer_id,
				gat.group_id,
				gat.count_discont/vp.group_purchase::NUMERIC AS Group_Discount_Share
			FROM g_amount_transaction gat
			JOIN v_periods vp ON vp.customer_id = gat.customer_id AND vp.group_id = gat.group_id
			GROUP BY gat.customer_id, gat.group_id, Group_Discount_Share),
		/* ОПРЕДЕЛЕНИЕ МИНИМАЛЬНОГО РАЗМЕРА СКИДКИ ПО ГРУППЕ
		 * Определяется
		минимальный размер скидки по каждой группе для каждого клиента. Для
		этого выбирается минимальное не равное нулю значение поля
		Group_Min_Discount таблицы Периоды для заданных клиента и
		группы. Результат сохраняется в поле Group_Minimum_Discount
		таблицы Группы.
		 * */
		g_minimum_discount AS (
			SELECT
				customer_id,
				group_id,
				min(group_min_discount) FILTER (WHERE group_min_discount > 0) AS Group_Minimum_Discount
			FROM v_periods
			GROUP BY customer_id, group_id),
		/* ОПРЕДЕЛЕНИЕ СРЕДНЕГО РАЗМЕРА СКИДКИ ПО ГРУППЕ
		 * Для определения
		среднего размера скидки по группе для клиента фактически оплаченная
		сумма по покупке группы в рамках всех транзакций (значение поля
		Group_Summ_Paid таблицы История покупок для всех транзакций)
		делится на сумму розничной стоимости данной группы в рамках всех
		транзакций (сумма по группе по значению поля Group_Summ таблицы
		История покупок). В расчете участвуют только транзакции, в которых была предоставлена скидка.
		Результат сохраняется в поле Group_Average_Discount таблицы Группы
		 * */
		g_average_discount AS (
			SELECT
				customer_id,
				group_id,
				avg(group_summ_paid / group_summ)::numeric AS Group_Average_Discount
			FROM v_purchase_history
			GROUP BY customer_id, group_id)
	SELECT
		gax.customer_id,
		gax.group_id,
		gax.group_affinity_index,
		gcr.group_churn_rate,
		gsi.group_stability_index,
		gm.group_margin,
		gds.group_discount_share,
		gmd.group_minimum_discount,
		gad.group_average_discount
	FROM g_affinity_index gax
	JOIN g_churn_rate gcr ON gcr.customer_id = gax.customer_id AND gcr.group_id = gax.group_id
	JOIN g_stability_index gsi ON gsi.customer_id = gax.customer_id AND gsi.group_id = gax.group_id
	JOIN g_margin gm ON gm.customer_id = gax.customer_id AND gm.group_id = gax.group_id
	JOIN g_discount_share gds ON gds.customer_id=gax.customer_id AND gds.group_id=gax.group_id
	JOIN g_minimum_discount gmd ON gmd.customer_id=gax.customer_id AND gmd.group_id=gax.group_id
	JOIN g_average_discount gad ON gad.customer_id=gax.customer_id AND gad.group_id=gax.group_id;
	

