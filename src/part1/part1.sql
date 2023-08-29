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



