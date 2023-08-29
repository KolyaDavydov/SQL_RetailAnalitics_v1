-- Создание базы данных
--CREATE DATABASE sql3;

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


