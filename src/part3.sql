SELECT rolname FROM pg_roles;

-- Создаём групповые роли visitors_grp и admins_grp для облегчения управления
-- TODO добавить права на таблицы
CREATE OR REPLACE PROCEDURE proc_add_users_groups() 
AS $$
BEGIN
    CREATE ROLE visitors_grp NOLOGIN;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO visitors_grp;
    CREATE ROLE admins_grp NOLOGIN;
    GRANT SELECT, UPDATE, INSERT, DELETE ON ALL TABLES IN SCHEMA public TO admins_grp;
    -- GRANT USAGE ON SCHEMA public TO admins_grp;
END;
$$ LANGUAGE plpgsql;

-- Процедура для создания пользователя и включения его в группу
-- (имя пользователя, пароль, роль Administrators или Visitors, результат выполнения)

CREATE OR REPLACE PROCEDURE proc_add_user(user_name IN VARCHAR, user_password IN VARCHAR, user_role IN VARCHAR, res OUT VARCHAR) 
AS $$
BEGIN
    res = 'Failed';
    CASE WHEN user_role = 'Administrators' THEN
            EXECUTE 'CREATE ROLE ' || user_name || ' WITH LOGIN PASSWORD ''' || user_password || '''';
            EXECUTE 'GRANT admins_grp TO ' || user_name;
        res = 'OK';
        WHEN user_role = 'Visitors' THEN
            EXECUTE 'CREATE ROLE ' || user_name || ' WITH LOGIN PASSWORD ''' || user_password || '''';
            EXECUTE 'GRANT visitors_grp TO ' || user_name;
            res = 'OK';
        ELSE RETURN;
    END CASE;
    RETURN;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL proc_add_users_groups();
-- Добавляем пользователя Administrator, даем ему права групповой роли
CALL proc_add_user('Administrator', 'super_pass111', 'Administrators',null);
-- Добавляем пользователя Visitor, даем ему права групповой роли
CALL proc_add_user('Visitor', '111_pass_super', 'Visitors',null);
END;

DROP ROLE Visitor;
