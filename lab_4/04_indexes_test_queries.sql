-- Лабораторная работа №4 - Тестирование производительности после создания индексов

-- Часть 1: Те же запросы, что и до индексов, для сравнения

-- Запрос 1: Поиск по диапазону значений (дата рождения) - ПОСЛЕ индексов
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM employees 
WHERE birth_date BETWEEN '1980-01-01' AND '1990-12-31';

-- Запрос 2: Фильтрация и сортировка по текстовым полям - ПОСЛЕ индексов
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM employees 
WHERE last_name LIKE 'Ива%' 
ORDER BY last_name, first_name;

-- Запрос 3: Поиск по подстроке с использованием LIKE - ПОСЛЕ индексов
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM departments 
WHERE description LIKE '%разработка%';

-- Запрос 4: JOIN с фильтрацией по дате - ПОСЛЕ индексов
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT e.first_name, e.last_name, d.name as department_name, a.start_date
FROM employees e
JOIN assignments a ON e.id = a.employee_id
JOIN departments d ON a.department_id = d.id
WHERE a.start_date >= '2020-01-01'
ORDER BY a.start_date DESC;

-- Часть 2: Сложные запросы для анализа производительности

-- Запрос 5: Агрегация с JOIN и группировкой
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT 
    d.name as department_name,
    COUNT(DISTINCT a.employee_id) as employee_count,
    AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, e.birth_date))) as avg_age
FROM departments d
LEFT JOIN assignments a ON d.id = a.department_id AND a.end_date IS NULL
LEFT JOIN employees e ON a.employee_id = e.id
GROUP BY d.id, d.name
ORDER BY employee_count DESC;

-- Запрос 6: Сложная фильтрация с подзапросами
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT 
    e.first_name,
    e.last_name,
    e.hire_date,
    (SELECT COUNT(*) FROM vacations v 
     WHERE v.employee_id = e.id AND v.status = 'approved') as approved_vacations,
    (SELECT AVG(hours) FROM work_hours wh 
     WHERE wh.employee_id = e.id AND wh.date >= '2023-10-01') as avg_hours
FROM employees e
WHERE e.id IN (
    SELECT DISTINCT a.employee_id 
    FROM assignments a 
    WHERE a.department_id = 1 AND a.start_date >= '2020-01-01'
)
ORDER BY e.hire_date DESC;

-- Запрос 7: Многотабличный JOIN с агрегацией и сортировкой
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT 
    e.first_name,
    e.last_name,
    d.name as department_name,
    p.title as position_title,
    COUNT(v.id) as total_vacations,
    SUM(EXTRACT(DAY FROM (v.end_date - v.start_date))) as total_vacation_days
FROM employees e
JOIN assignments a ON e.id = a.employee_id
JOIN departments d ON a.department_id = d.id
JOIN positions p ON a.position_id = p.id
LEFT JOIN vacations v ON e.id = v.employee_id AND v.status = 'approved'
WHERE a.end_date IS NULL
GROUP BY e.id, e.first_name, e.last_name, d.name, p.title
HAVING COUNT(v.id) > 0
ORDER BY total_vacation_days DESC;

-- Анализ использования индексов
SELECT 
    tablename,
    indexname,
    idx_scan as index_scans,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched
FROM pg_stat_user_indexes 
WHERE schemaname = 'public'
ORDER BY idx_scan DESC;
