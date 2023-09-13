DROP FUNCTION IF EXISTS fn_main_part6(
	integer,
	NUMERIC,
	NUMERIC,
	NUMERIC,
	NUMERIC);

CREATE OR REPLACE FUNCTION fn_main_part6 (
	count_groups integer,
	max_churn_rate NUMERIC,
	max_stability_index NUMERIC,
	max_sku_share NUMERIC,
	margin_share NUMERIC)
RETURNS TABLE (
	Customer_ID_ integer,
	SKU_name_ varchar,
	Offer_Discount_Depth NUMERIC)
AS $$
BEGIN
	RETURN query
		(WITH
			/* Выбор групп.
			 * Для формирования предложений, ориентированных на
			рост маржи за счет кросс-продаж, для каждого клиента выбирается несколько
			групп (количество задается пользователем) с
			максимальным индексом востребованности, отвечающие следующим
			условиям:
			
			Индекс оттока по группе не более заданного пользователем значения.
			
			Индекс стабильности потребления группы составляет менее заданного пользователем значения.
			 * */
			tmp1 AS (
				SELECT
					customer_id,
					group_id,
					ROW_NUMBER() OVER (PARTITION BY customer_id) AS row_num, --считаем количество групп
					group_minimum_discount
				FROM v_groups
				WHERE group_churn_rate <= max_churn_rate AND group_stability_index < max_stability_index
				ORDER BY group_affinity_index  DESC),
			tmp2 AS (
				SELECT customer_id, group_id, group_minimum_discount
				FROM tmp1
				WHERE row_num < count_groups),
			/* Определение SKU с максимальной маржой. В каждой группе
			определяется SKU с максимальной маржой (в рублях). Для этого по
			основному магазину клиента из розничной цены товара
			(SKU_Retail_Price) вычитается его закупочная стоимость
			(SKU_Purchase_Price) для всех SKU данной группы, представленных
			в магазине, после чего выбирается одно SKU с максимальным
			значением указанной разницы.
			 * 
			 * */
			tmp3 AS (
				SELECT DISTINCT
					tmp2.customer_id,
					tmp2.group_id,
					sku.sku_id,
					sku.sku_name,
					stores.sku_retail_price-stores.sku_purchase_price AS delta,
					stores.sku_retail_price,
					v_customers.customer_primary_store,
					group_minimum_discount
				FROM tmp2
				JOIN v_customers ON v_customers.customer_id = tmp2.customer_id
				JOIN sku ON sku.group_id=tmp2.group_id
				JOIN stores ON stores.sku_id=sku.sku_id),
			tmp4 AS (
				SELECT
					customer_id,
					group_id,
					sku_id,
					sku_name,
					delta,
					sku_retail_price,
					group_minimum_discount,
					customer_primary_store,
					rank() OVER (PARTITION BY customer_id, group_id, customer_primary_store ORDER BY delta DESC) AS ran -- добавляем столбец с рангом, если == 1 то это точто нам нужно - максимальное 
				FROM tmp3),
			tmp5 AS (
				SELECT
					customer_id,
					group_id,
					sku_id,
					sku_name,
					delta,
					customer_primary_store,
					sku_retail_price,
					group_minimum_discount
				FROM tmp4
				WHERE ran < 2),
			/*Определение доли SKU в группе. Определяется доля транзакций, в
			которых присутствует анализируемое SKU. Для этого количество
			транзакций, содержащих данный SKU, делится на количество
			транзакций, содержащих группу в целом (за анализируемый период).
			SKU используется для формирования предложения только в том случае,
			если получившееся значение не превышает заданного пользователем значения.
			 * */
			tmp6 AS (
				SELECT DISTINCT
					tmp5.customer_id,
					group_id,
					tmp5.sku_id,
					sku_name,
					delta,
					customer_primary_store,
					transaction_id,
					sku_retail_price,
					group_minimum_discount,
					(c_ts::NUMERIC / c_tg) * 100 AS value -- доля маржи в процентах
				FROM tmp5
				JOIN checks c ON tmp5.sku_id = c.sku_id
				LEFT JOIN (
					SELECT count(transaction_id) AS c_ts,
						sku_id AS c_s
					FROM checks
					GROUP BY sku_id) AS ff ON tmp5.sku_id = ff.c_s
				LEFT JOIN (
					SELECT group_id AS c_g,
						count(transaction_id) AS c_tg
					FROM checks
					JOIN sku ON checks.sku_id=sku.sku_id
					GROUP BY group_id) AS fff ON group_id=fff.c_g
				ORDER BY customer_id, group_id),
				/*Расчет скидки. Заданное пользователем на шаге 4 значение
				умножается на разницу между розничной (SKU_Retail_Price) и
				закупочной (SKU_Purchase_Price) ценой, а получившееся значение
				делится на розничную цену SKU (SKU_Retail_Price). Все цены – для
				основного магазина клиента. В случае, если получившееся значение
				равно или превышает минимальный размер скидки пользователя для
				анализируемой группы, округленной вверх с шагом в 5%, то в
				качестве скидки для данного SKU для клиента устанавливается
				минимальная скидка для группы, округленная вверх с шагом в 5%. В
				противном случае для клиента не формируется предложение по данной
				группе.
				 * */
			tmp7 AS (
				SELECT
					customer_id,
					sku_name,
					delta * margin_share / sku_retail_price AS ttmp,
					ceil(group_minimum_discount::NUMERIC * 100 / 5) * 5 AS disc
				FROM tmp6
				WHERE value <= max_sku_share),
			tmp8 AS (
				SELECT DISTINCT
					customer_id,
					sku_name,
					disc
				FROM tmp7
				WHERE ttmp * 100 >= disc)
		SELECT * FROM tmp8 order by 1);
END
$$language plpgsql;


--Параметры функции:
--
-- 1 -количество групп
-- 2 - максимальный индекс оттока
-- 3 - максимальный индекс стабильности потребления
-- 4 - максимальная доля SKU (в процентах)
-- 5 - допустимая доля маржи (в процентах)
SELECT *
FROM fn_main_part6(5, 3, 0.5, 100, 30);