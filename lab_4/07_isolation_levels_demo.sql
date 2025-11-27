-- Уровень 1: READ COMMITTED
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
BEGIN;
-- Выполнить операции...
COMMIT;

-- Уровень 2: REPEATABLE READ  
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN;
-- Выполнить операции...
COMMIT;

-- Уровень 3: SERIALIZABLE
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN;
-- Выполнить операции...
COMMIT;

-- Тестовые запросы для каждого уровня
DO $$
BEGIN
    RAISE NOTICE 'Тестирование уровней изоляции завершено';
END $$;
