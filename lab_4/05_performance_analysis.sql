-- Лабораторная работа №4 - Демонстрация транзакций и аномалий

-- СЦЕНАРИЙ 1: Dirty Read (Грязное чтение)
-- В PostgreSQL READ UNCOMMITTED не допускает Dirty Read, но покажем попытку

-- Окно 1:
BEGIN;
UPDATE employees SET first_name = 'НовоеИмяГрязное' WHERE id = 1;
-- НЕ ВЫПОЛНЯТЬ COMMIT!

-- Окно 2:
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
BEGIN;
SELECT first_name FROM employees WHERE id = 1; -- Увидим старое значение в PostgreSQL
COMMIT;

-- Окно 1:
ROLLBACK;

-- СЦЕНАРИЙ 2: Non-repeatable Read (Неповторяющееся чтение)

-- Окно 1:
BEGIN;
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
SELECT first_name FROM employees WHERE id = 2; -- Первое чтение

-- Окно 2:
BEGIN;
UPDATE employees SET first_name = 'ИзмененноеИмя' WHERE id = 2;
COMMIT;

-- Окно 1:
SELECT first_name FROM employees WHERE id = 2; -- Второе чтение (разные результаты!)
COMMIT;

-- Вернем исходное значение
UPDATE employees SET first_name = 'Петр' WHERE id = 2;

-- СЦЕНАРИЙ 3: Phantom Read (Фантомное чтение)

-- Окно 1:
BEGIN;
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
SELECT COUNT(*) FROM employees WHERE hire_date >= '2023-01-01'; -- Первое чтение

-- Окно 2:
BEGIN;
INSERT INTO employees (first_name, last_name, birth_date, email, hire_date) 
VALUES ('Новый', 'Сотрудник', '1995-01-01', 'new2023@company.com', '2023-12-01');
COMMIT;

-- Окно 1:
SELECT COUNT(*) FROM employees WHERE hire_date >= '2023-01-01'; -- Второе чтение (другое количество!)
COMMIT;

-- Очистка тестовых данных
DELETE FROM employees WHERE email = 'new2023@company.com';

-- Демонстрация решения аномалий через уровни изоляции

-- Решение для Non-repeatable Read:
BEGIN;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SELECT first_name FROM employees WHERE id = 2; -- Первое чтение

-- В другом окне попробуем изменить данные:
-- UPDATE employees SET first_name = 'ПопыткаИзменения' WHERE id = 2; -- Будет ждать или завершится с ошибкой

COMMIT;

-- Решение для Phantom Read:
BEGIN;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SELECT COUNT(*) FROM employees WHERE hire_date >= '2023-01-01';

-- В другом окне:
-- INSERT INTO employees ... -- Будет заблокировано или вызовет конфликт сериализации

COMMIT;

-- Информация о текущих настройках изоляции
SHOW default_transaction_isolation;
