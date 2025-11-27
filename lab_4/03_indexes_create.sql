-- Индексы для поиска по диапазону значений
CREATE INDEX IF NOT EXISTS idx_employees_birth_date ON employees(birth_date);
CREATE INDEX IF NOT EXISTS idx_employees_hire_date ON employees(hire_date);

-- Индексы для текстовых полей
CREATE INDEX IF NOT EXISTS idx_employees_last_name ON employees(last_name);

-- Индексы для JOIN операций
CREATE INDEX IF NOT EXISTS idx_assignments_employee_id ON assignments(employee_id);
CREATE INDEX IF NOT EXISTS idx_assignments_department_id ON assignments(department_id);

-- Составные индексы
CREATE INDEX IF NOT EXISTS idx_assignments_employee_dates ON assignments(employee_id, start_date, end_date);

-- Комментарии к индексам
COMMENT ON INDEX idx_employees_birth_date IS 'Индекс для поиска сотрудников по дате рождения';
COMMENT ON INDEX idx_employees_hire_date IS 'Индекс для поиска по дате приема на работу';
