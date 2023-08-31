-- Создание базы данных
-- CREATE DATABASE sql3;

-- Таблица Персональные данные:
CREATE TABLE IF NOT EXISTS person_data (
	Customer_ID				integer	PRIMARY KEY,
	Customer_Name			varchar	CHECK (Customer_Name ~ '(^[A-Z]([a-z]|-|\s)*|^[А-Я]([а-я]|-|\s)*)'),
	Customer_Surname		varchar	CHECK (Customer_Surname ~ '(^[A-Z]([a-z]|-|\s)*|^[А-Я]([а-я]|-|\s)*)'),
	Customer_primary_Email	varchar CHECK (Customer_Primary_Email ~ '^[A-Za-z0-9._+%-]+@[A-Za-z0-9.-]+[.][A-Za-z]+$'),
	Customer_Primary_Phone 	varchar CHECK (Customer_Primary_Phone ~ '^\+7\d{10}$')
);

-- Таблица Карты
CREATE TABLE IF NOT EXISTS cards (
	Customer_Card_ID	integer PRIMARY KEY,
	Customer_ID			integer,
	
	FOREIGN KEY (Customer_ID) REFERENCES person_data (Customer_ID)
);
COMMENT ON COLUMN cards.Customer_ID IS 'Одному клиенту может принадлежать несколько карт';

-- Таблица Транзакции
CREATE TABLE IF NOT EXISTS transactions (
	Transaction_ID			integer		PRIMARY KEY,
	Customer_Card_ID		integer,
	Transaction_Summ		numeric,
	Transaction_DateTime	timestamp,
	Transaction_Store_ID	integer,
	
	FOREIGN KEY (Customer_Card_ID) REFERENCES Cards (Customer_Card_ID)
);
COMMENT ON COLUMN transactions.Transaction_ID 		IS 'Уникальное значение';
COMMENT ON COLUMN transactions.Transaction_Summ 	IS 'Сумма транзакции в рублях (полная стоимость покупки без учета скидок)';
COMMENT ON COLUMN transactions.Transaction_DateTime	IS 'Дата и время совершения транзакции';
COMMENT ON COLUMN transactions.Transaction_ID 		IS 'Магазин, в котором была совершена транзакция';

-- Таблица Группы SKU
CREATE TABLE groups_sku (
	Group_ID	integer	PRIMARY KEY,
	Group_Name	varchar	CHECK (Group_Name ~ '^[A-zА-я0-9_\/\s-]+$')
);

-- Таблица Товарная матрица
CREATE TABLE sku (
	SKU_ID		integer	PRIMARY KEY,
	SKU_Name	varchar	CHECK (sku_name ~ '^[A-zА-я0-9_\/\s-]+$'),
	Group_ID	integer,
	
	FOREIGN KEY (Group_ID) REFERENCES Groups_SKU (Group_ID)
);

-- Таблица Чеки
CREATE TABLE IF NOT EXISTS checks (
	Transaction_ID	integer,
	SKU_ID			integer,
	SKU_Amount		numeric,
	SKU_Summ		numeric,
	SKU_Summ_Paid	numeric,
	SKU_Discount	numeric,
	
    FOREIGN KEY (Transaction_ID) REFERENCES Transactions (Transaction_ID),
    FOREIGN KEY (SKU_ID) REFERENCES SKU (SKU_ID)
);
COMMENT ON COLUMN checks.Transaction_ID	IS 'Идентификатор транзакции указывается для всех позиций в чеке';
COMMENT ON COLUMN checks.SKU_Amount 	IS 'Указание, какое количество товара было куплено';
COMMENT ON COLUMN checks.SKU_Summ		IS 'Сумма покупки фактического объема данного товара в рублях (полная стоимость без учета скидок и бонусов)';
COMMENT ON COLUMN checks.SKU_Summ_Paid	IS 'Фактически оплаченная сумма покупки данного товара, не включая сумму предоставленной скидки';
COMMENT ON COLUMN checks.SKU_Discount	IS 'Размер предоставленной на товар скидки в рублях';

-- Таблица торговые точки
CREATE TABLE Stores (
	Transaction_Store_ID	integer,
	SKU_ID					integer,
	SKU_Purchase_Price		numeric,
	SKU_Retail_Price		numeric,
    
	FOREIGN KEY (Transaction_Store_ID) REFERENCES transactions(Transaction_ID),
	FOREIGN KEY (SKU_ID) REFERENCES SKU (SKU_ID)
);
COMMENT ON COLUMN Stores.SKU_Purchase_Price IS 'Закупочная стоимость товара для данного магазина';
COMMENT ON COLUMN Stores.SKU_Retail_Price IS 'Стоимость продажи товара без учета скидок для данного магазина';

--Таблица Дата формирования анализа
CREATE TABLE Date_Of_Analysis_Formation (
	Analysis_Formation timestamp WITHOUT time ZONE
);
