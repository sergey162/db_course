-- Лабораторная работа №4 - Анализ и создание индексов

-- Часть 1: Анализ запросов ДО создания индексов

-- Запрос 1: Поиск по диапазону значений (дата рождения)
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM employees 
WHERE birth_date BETWEEN '1980-01-01' AND '1990-12-31';

-- Запрос 2: Фильтрация и сортировка по текстовым полям
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM employees 
WHERE last_name LIKE 'Ива%' 
ORDER BY last_name, first_name;

-- Запрос 3: Поиск по подстроке с использованием LIKE
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM departments 
WHERE description LIKE '%разработка%';

-- Запрос 4: JOIN с фильтрацией по дате
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT e.first_name, e.last_name, d.name as department_name, a.start_date
FROM employees e
JOIN assignments a ON e.id = a.employee_id
JOIN departments d ON a.department_id = d.id
WHERE a.start_date >= '2020-01-01'
ORDER BY a.start_date DESC;

-- Часть 2: Создание индексов

-- Индексы для поиска по диапазону значений
CREATE INDEX idx_employees_birth_date ON employees(birth_date);
CREATE INDEX idx_employees_hire_date ON employees(hire_date);
CREATE INDEX idx_assignments_start_date ON assignments(start_date);
CREATE INDEX idx_assignments_end_date ON assignments(end_date);
CREATE INDEX idx_vacations_start_date ON vacations(start_date);
CREATE INDEX idx_vacations_end_date ON vacations(end_date);

-- Индексы для текстовых полей (фильтрация и сортировка)
CREATE INDEX idx_employees_last_name ON employees(last_name);
CREATE INDEX idx_employees_first_name ON employees(first_name);
CREATE INDEX idx_departments_name ON departments(name);
CREATE INDEX idx_positions_title ON positions(title);

-- Индексы для поиска по подстроке (частичное индексирование)
CREATE INDEX idx_departments_description_trgm ON departments USING gin (description gin_trgm_ops);
CREATE INDEX idx_positions_description_trgm ON positions USING gin (description gin_trgm_ops);

-- Индексы для внешних ключей (улучшение JOIN)
CREATE INDEX idx_assignments_employee_id ON assignments(employee_id);
CREATE INDEX idx_assignments_department_id ON assignments(department_id);
CREATE INDEX idx_assignments_position_id ON assignments(position_id);
CREATE INDEX idx_vacations_employee_id ON vacations(employee_id);
CREATE INDEX idx_work_hours_employee_id ON work_hours(employee_id);

-- Составные индексы для часто используемых комбинаций
CREATE INDEX idx_employees_name_composite ON employees(last_name, first_name);
CREATE INDEX idx_assignments_employee_dates ON assignments(employee_id, start_date, end_date);
CREATE INDEX idx_vacations_employee_status ON vacations(employee_id, status);
CREATE INDEX idx_work_hours_employee_date ON work_hours(employee_id, date);

-- Комментарии к индексам
COMMENT ON INDEX idx_employees_birth_date IS 'Индекс для поиска сотрудников по дате рождения';
COMMENT ON INDEX idx_employees_hire_date IS 'Индекс для поиска по дате приема на работу';
COMMENT ON INDEX idx_employees_last_name IS 'Индекс для фильтрации и сортировки по фамилии';
COMMENT ON INDEX idx_departments_description_trgm IS 'GIN индекс для полнотекстового поиска в описании отделов';

-- Вывод информации о созданных индексах
SELECT 
    tablename,
    indexname,
    indexdef
FROM pg_indexes 
WHERE schemaname = 'public'
ORDER BY tablename, indexname;
