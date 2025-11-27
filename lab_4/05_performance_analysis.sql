-- Запрос 1: Агрегация с JOIN
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT 
    d.name as department_name,
    COUNT(DISTINCT a.employee_id) as employee_count,
    AVG(EXTRACT(YEAR FROM AGE(e.birth_date))) as avg_age
FROM departments d
LEFT JOIN assignments a ON d.id = a.department_id
LEFT JOIN employees e ON a.employee_id = e.id
WHERE a.start_date >= '2020-01-01'
GROUP BY d.id, d.name
ORDER BY employee_count DESC;

-- Запрос 2: Сложная фильтрация с подзапросами
EXPLAIN (ANALYZE, BUFFERS)
SELECT 
    e.*,
    (SELECT COUNT(*) FROM vacations v 
     WHERE v.employee_id = e.id AND v.status = 'approved') as vacation_count
FROM employees e
WHERE EXISTS (
    SELECT 1 FROM assignments a 
    WHERE a.employee_id = e.id 
    AND a.start_date >= '2023-01-01'
)
ORDER BY e.hire_date DESC;
