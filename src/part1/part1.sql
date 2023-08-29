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

