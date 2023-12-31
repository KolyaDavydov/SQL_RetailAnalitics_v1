
-- ИМПОРТ .csv и .tsv файлов

--процедура импорта файлов

CREATE OR REPLACE PROCEDURE import(table_name varchar, path text, delim character DEFAULT '\t')
AS $$
    BEGIN
        IF (delim = '\t') THEN -- если табуляция то .tsv файл
            EXECUTE concat('COPY ', table_name, ' FROM ''', path, ''' DELIMITER E''\t''', ';');
        ELSE
            EXECUTE concat('COPY ', table_name, ' FROM ''', path, ''' DELIMITER ''', delim, ';');
        END IF;
    END;
    $$ LANGUAGE plpgsql;

-- что б данные с временем не ругались
SET DATESTYLE to iso, DMY;

--устанваливаем параметр пути где находятся Файлы
--!!!
--ПЕРЕД ДОБАВЛЕНИЕМ ДАННЫХ В ТАБЛИЦУ ОБЯЗАТЕЛЬНО ИЗМЕНИТЕ ПУТЬ НА СВОЙ АБСОЛЮТНЫЙ
--В АБСОЛЮТНОМ ПУТИ НЕ ДОЛЖНО БЫТЬ КИРРИЛИЦЫ инваче возможны проблемы!!!
SET import_path.txt TO 'C:\Nikolay\CSV\datasets\';

-- заполняем данными таблицы из датасета
CALL import('Person_Data', (current_setting('import_path.txt') || 'Personal_Data_Mini.tsv')); -- 'curent_setting' - выдает текущее значение параметра
CALL import('Cards', (current_setting('import_path.txt') || 'Cards_Mini.tsv'));
CALL import('Transactions', (current_setting('import_path.txt') || 'Transactions_Mini.tsv'));
CALL import('Groups_SKU', (current_setting('import_path.txt') || 'Groups_SKU_Mini.tsv'));
CALL import('SKU', (current_setting('import_path.txt') || 'SKU_Mini.tsv'));
CALL import('Checks', (current_setting('import_path.txt') || 'Checks_Mini.tsv'));
CALL import('Date_Of_Analysis_Formation', (current_setting('import_path.txt') || 'Date_Of_Analysis_Formation.tsv'));
CALL import('Stores', (current_setting('import_path.txt') || 'Stores_Mini.tsv'));

-- заполняем данными таблицы из БОЛЬШОГО датасета
-- CALL import('Person_Data', (current_setting('import_path.txt') || 'Personal_Data.tsv')); -- 'curent_setting' - выдает текущее значение параметра
-- CALL import('Cards', (current_setting('import_path.txt') || 'Cards.tsv'));
-- CALL import('Transactions', (current_setting('import_path.txt') || 'Transactions.tsv'));
-- CALL import('Groups_SKU', (current_setting('import_path.txt') || 'Groups_SKU.tsv'));
-- CALL import('SKU', (current_setting('import_path.txt') || 'SKU.tsv'));
-- CALL import('Checks', (current_setting('import_path.txt') || 'Checks.tsv'));
-- CALL import('Date_Of_Analysis_Formation', (current_setting('import_path.txt') || 'Date_Of_Analysis_Formation.tsv'));
-- CALL import('Stores', (current_setting('import_path.txt') || 'Stores.tsv'));