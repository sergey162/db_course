-- ЗАДАНИЕ 1: ИНДЕКСЫ

EXPLAIN ANALYZE
SELECT * FROM work_hours 
WHERE date = '2023-01-03' AND hours = 8.5;

CREATE INDEX idx_wh_composite ON work_hours(date, hours);

SET enable_seqscan = OFF;
EXPLAIN ANALYZE
SELECT * FROM work_hours 
WHERE date = '2023-01-03' AND hours = 8.5;
SET enable_seqscan = ON;

EXPLAIN ANALYZE
SELECT d.name, COUNT(e.id), SUM(wh.hours)
FROM departments d
JOIN assignments a ON d.id = a.department_id
JOIN employees e ON a.employee_id = e.id
JOIN work_hours wh ON e.id = wh.employee_id
WHERE d.name = 'Department 10'
GROUP BY d.name;

CREATE INDEX idx_dept_name ON departments(name);
CREATE INDEX idx_assign_dept ON assignments(department_id);
CREATE INDEX idx_assign_emp ON assignments(employee_id);
CREATE INDEX idx_wh_emp ON work_hours(employee_id);

SET enable_seqscan = OFF;
EXPLAIN ANALYZE
SELECT d.name, COUNT(e.id), SUM(wh.hours)
FROM departments d
JOIN assignments a ON d.id = a.department_id
JOIN employees e ON a.employee_id = e.id
JOIN work_hours wh ON e.id = wh.employee_id
WHERE d.name = 'Department 10'
GROUP BY d.name;
SET enable_seqscan = ON;


-- Сценарий 1: Non-repeatable Read
-- [T1]
-- BEGIN ISOLATION LEVEL READ COMMITTED;
-- SELECT hours FROM work_hours WHERE id = 1; -- (например 10.0)
    -- [T2]
    -- BEGIN;
    -- UPDATE work_hours SET hours = 20.0 WHERE id = 1;
    -- COMMIT;
-- [T1]
-- SELECT hours FROM work_hours WHERE id = 1; -- (стало 20.0 -> Аномалия)
-- COMMIT;


-- Сценарий 2: Phantom Read
-- [T1]
-- BEGIN ISOLATION LEVEL READ COMMITTED;
-- SELECT COUNT(*) FROM departments; 
    -- [T2]
    -- BEGIN;
    -- INSERT INTO departments (name) VALUES ('Phantom Dept');
    -- COMMIT;
-- [T1]
-- SELECT COUNT(*) FROM departments; -- (Число изменилось -> Аномалия)
-- COMMIT;


-- Сценарий 3: Dirty Read
-- [T1]
-- BEGIN ISOLATION LEVEL READ UNCOMMITTED;
    -- [T2]
    -- BEGIN;
    -- UPDATE work_hours SET hours = 23.0 WHERE id = 1; 
    -- (БЕЗ COMMIT)
-- [T1]
-- SELECT hours FROM work_hours WHERE id = 1; 
-- (Должно вернуть старое значение 10.0, грязные данные не видны)
-- COMMIT;
    -- [T2]
    -- ROLLBACK;