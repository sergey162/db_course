-- Сценарий 1: Dirty Read
-- В ПЕРВОМ ОКНЕ PSQL:
BEGIN;
UPDATE employees SET first_name = 'НовоеИмя' WHERE id = 1;
-- НЕ ВЫПОЛНЯТЬ COMMIT!

-- ВО ВТОРОМ ОКНЕ PSQL:
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
BEGIN;
SELECT first_name FROM employees WHERE id = 1; -- Dirty Read
COMMIT;

-- В ПЕРВОМ ОКНЕ:
ROLLBACK;

-- Сценарий 2: Non-repeatable Read
-- В ПЕРВОМ ОКНЕ:
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
BEGIN;
SELECT first_name FROM employees WHERE id = 1; -- Первое чтение

-- ВО ВТОРОМ ОКНЕ:
BEGIN;
UPDATE employees SET first_name = 'Измененное' WHERE id = 1;
COMMIT;

-- В ПЕРВОМ ОКНЕ:
SELECT first_name FROM employees WHERE id = 1; -- Второе чтение (разные результаты)
COMMIT;
