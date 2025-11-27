-- Лабораторная работа №4 - Демонстрация уровней изоляции транзакций

-- Уровень 1: READ UNCOMMITTED (в PostgreSQL работает как READ COMMITTED)
DO $$
BEGIN
    RAISE NOTICE '=== Уровень изоляции: READ UNCOMMITTED ===';
END $$;

-- Окно 1:
BEGIN ISOLATION LEVEL READ UNCOMMITTED;
UPDATE employees SET phone = '+79991111111' WHERE id = 3;
-- Не коммитить!

-- Окно 2:
BEGIN ISOLATION LEVEL READ UNCOMMITTED;
SELECT phone FROM employees WHERE id = 3; -- Не увидит незакоммиченных изменений в PostgreSQL
COMMIT;

-- Окно 1:
ROLLBACK;

-- Уровень 2: READ COMMITTED (уровень по умолчанию)
DO $$
BEGIN
    RAISE NOTICE '=== Уровень изоляции: READ COMMITTED ===';
END $$;

-- Окно 1:
BEGIN ISOLATION LEVEL READ COMMITTED;
SELECT first_name FROM employees WHERE id = 4;

-- Окно 2:
BEGIN;
UPDATE employees SET first_name = 'ИзменениеПриReadCommitted' WHERE id = 4;
COMMIT;

-- Окно 1:
SELECT first_name FROM employees WHERE id = 4; -- Увидит изменения после коммита
COMMIT;

-- Вернем исходное значение
UPDATE employees SET first_name = 'Анна' WHERE id = 4;

-- Уровень 3: REPEATABLE READ
DO $$
BEGIN
    RAISE NOTICE '=== Уровень изоляции: REPEATABLE READ ===';
END $$;

-- Окно 1:
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT first_name FROM employees WHERE id = 5;

-- Окно 2:
BEGIN;
UPDATE employees SET first_name = 'ПопыткаИзменения' WHERE id = 5;
COMMIT;

-- Окно 1:
SELECT first_name FROM employees WHERE id = 5; -- Не увидит изменения (гарантия повторяемости)
COMMIT;

-- Уровень 4: SERIALIZABLE
DO $$
BEGIN
    RAISE NOTICE '=== Уровень изоляции: SERIALIZABLE ===';
END $$;

-- Окно 1:
BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT COUNT(*) FROM employees WHERE hire_date > '2022-01-01';

-- Окно 2:
BEGIN ISOLATION LEVEL SERIALIZABLE;
INSERT INTO employees (first_name, last_name, birth_date, email, hire_date) 
VALUES ('Сериализуемый', 'Тест', '1990-01-01', 'serializable@test.com', '2023-01-01');
COMMIT;

-- Окно 1:
SELECT COUNT(*) FROM employees WHERE hire_date > '2022-01-01'; -- Может вызвать ошибку сериализации
COMMIT;

-- Очистка тестовых данных
DELETE FROM employees WHERE email = 'serializable@test.com';

-- Сравнительная таблица уровней изоляции
DO $$
BEGIN
    RAISE NOTICE ' ';
    RAISE NOTICE '=== СРАВНЕНИЕ УРОВНЕЙ ИЗОЛЯЦИИ ===';
    RAISE NOTICE 'READ UNCOMMITTED: Грязное чтение - нет, Неповторяющееся - да, Фантомы - да';
    RAISE NOTICE 'READ COMMITTED:   Грязное чтение - нет, Неповторяющееся - да, Фантомы - да';
    RAISE NOTICE 'REPEATABLE READ:  Грязное чтение - нет, Неповторяющееся - нет, Фантомы - да';
    RAISE NOTICE 'SERIALIZABLE:     Грязное чтение - нет, Неповторяющееся - нет, Фантомы - нет';
    RAISE NOTICE ' ';
END $$;

-- Практический пример выбора уровня изоляции
DO $$
BEGIN
    RAISE NOTICE '=== ПРАКТИЧЕСКИЕ РЕКОМЕНДАЦИИ ===';
    RAISE NOTICE 'READ COMMITTED:   Подходит для большинства операций чтения';
    RAISE NOTICE 'REPEATABLE READ:  Для отчетов, где важна консистентность данных';
    RAISE NOTICE 'SERIALIZABLE:     Для финансовых операций, где критична абсолютная консистентность';
END $$;
