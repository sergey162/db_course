-- Тест 1: Поиск по диапазону дат
EXPLAIN (ANALYZE, BUFFERS) 
SELECT * FROM employees 
WHERE birth_date BETWEEN '1980-01-01' AND '1990-12-31';

-- Тест 2: Фильтрация по текстовому полю
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM employees 
WHERE last_name LIKE 'Ива%' 
ORDER BY last_name, first_name;

-- Тест 3: JOIN с фильтрацией
EXPLAIN (ANALYZE, BUFFERS)
SELECT e.first_name, e.last_name, d.name 
FROM employees e
JOIN assignments a ON e.id = a.employee_id
JOIN departments d ON a.department_id = d.id
WHERE e.hire_date >= '2020-01-01';
