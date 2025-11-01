-- 1. DDL-скрипты
CREATE TABLE departments (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT
);

CREATE TABLE positions (
    id SERIAL PRIMARY KEY,
    title VARCHAR(100) NOT NULL,
    description TEXT,
    grade_level VARCHAR(50)
);

CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    birth_date DATE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    hire_date DATE NOT NULL
);

CREATE TABLE assignments (
    id SERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
    department_id INTEGER NOT NULL REFERENCES departments(id) ON DELETE CASCADE,
    position_id INTEGER NOT NULL REFERENCES positions(id) ON DELETE CASCADE,
    start_date DATE NOT NULL,
    end_date DATE,
    CONSTRAINT valid_dates CHECK (start_date <= end_date OR end_date IS NULL)
);

CREATE TABLE vacations (
    id SERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
    type VARCHAR(50) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    CONSTRAINT valid_vacation_dates CHECK (start_date <= end_date)
);

CREATE TABLE work_hours (
    id SERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    hours DECIMAL(5,2) NOT NULL CHECK (hours >= 0 AND hours <= 24),
    UNIQUE(employee_id, date)
);

-- 2. Наполнение базы данных
INSERT INTO departments (name, description) VALUES 
('IT', 'Информационные технологии'),
('HR', 'Отдел кадров'),
('Finance', 'Финансовый отдел');

INSERT INTO positions (title, description, grade_level) VALUES 
('Senior Developer', 'Старший разработчик', 'L5'),
('HR Manager', 'Менеджер по персоналу', 'L4'),
('Accountant', 'Бухгалтер', 'L3');

INSERT INTO employees (first_name, last_name, birth_date, email, phone, hire_date) VALUES 
('Иван', 'Петров', '1990-05-15', 'ivan.petrov@company.com', '+79161234567', '2020-03-01'),
('Мария', 'Сидорова', '1985-08-22', 'maria.sidorova@company.com', '+79167654321', '2019-07-15'),
('Алексей', 'Козлов', '1992-11-30', 'alex.kozlov@company.com', '+79169998877', '2021-01-10');

INSERT INTO assignments (employee_id, department_id, position_id, start_date, end_date) VALUES 
(1, 1, 1, '2020-03-01', NULL),
(2, 2, 2, '2019-07-15', NULL),
(3, 3, 3, '2021-01-10', NULL);

INSERT INTO vacations (employee_id, type, start_date, end_date, status) VALUES 
(1, 'ежегодный', '2024-06-01', '2024-06-14', 'approved'),
(2, 'больничный', '2024-05-10', '2024-05-12', 'approved'),
(3, 'административный', '2024-04-01', '2024-04-01', 'pending');

INSERT INTO work_hours (employee_id, date, hours) VALUES 
(1, '2024-05-01', 8.0),
(1, '2024-05-02', 7.5),
(2, '2024-05-01', 8.0),
(3, '2024-05-01', 6.0);

-- 3. Простые DML-операции
-- Вставка нового сотрудника
INSERT INTO employees (first_name, last_name, birth_date, email, hire_date) 
VALUES ('Светлана', 'Иванова', '1993-04-18', 'svetlana.ivanova@company.com', '2024-05-01');

-- Обновление телефона сотрудника
UPDATE employees SET phone = '+79165556677' WHERE email = 'svetlana.ivanova@company.com';

-- Удаление отпуска
DELETE FROM vacations WHERE status = 'pending' AND end_date < CURRENT_DATE;

-- 4. Запросы с агрегацией
-- Общее количество сотрудников по отделам
SELECT d.name, COUNT(a.employee_id) as employee_count
FROM departments d
LEFT JOIN assignments a ON d.id = a.department_id AND a.end_date IS NULL
GROUP BY d.id, d.name
HAVING COUNT(a.employee_id) > 0
ORDER BY employee_count DESC;

-- Среднее количество рабочих часов по сотрудникам
SELECT e.first_name, e.last_name, AVG(wh.hours) as avg_hours
FROM employees e
JOIN work_hours wh ON e.id = wh.employee_id
GROUP BY e.id, e.first_name, e.last_name
HAVING AVG(wh.hours) > 7.0
ORDER BY avg_hours DESC;

-- Статистика по отпускам
SELECT type, COUNT(*) as count, 
       AVG(end_date - start_date) as avg_duration,
       MIN(start_date) as earliest_vacation
FROM vacations
GROUP BY type;

-- 5. Запросы с соединениями таблиц
-- Сотрудники с их текущими отделами и должностями
SELECT e.first_name, e.last_name, d.name as department, p.title as position
FROM employees e
JOIN assignments a ON e.id = a.employee_id AND a.end_date IS NULL
JOIN departments d ON a.department_id = d.id
JOIN positions p ON a.position_id = p.id;

-- Отпуска с информацией о сотрудниках
SELECT e.first_name, e.last_name, v.type, v.start_date, v.end_date, v.status
FROM vacations v
LEFT JOIN employees e ON v.employee_id = e.id
WHERE v.status = 'approved';

-- 6. Создание представлений
-- Представление "Текущие сотрудники с детальной информацией"
CREATE VIEW current_employees_detail AS
SELECT 
    e.id,
    e.first_name,
    e.last_name,
    e.email,
    d.name as department,
    p.title as position,
    p.grade_level,
    a.start_date as assignment_start
FROM employees e
JOIN assignments a ON e.id = a.employee_id AND a.end_date IS NULL
JOIN departments d ON a.department_id = d.id
JOIN positions p ON a.position_id = p.id;

-- Представление "Статистика рабочих часов по месяцам"
CREATE VIEW monthly_work_hours AS
SELECT 
    e.id as employee_id,
    e.first_name,
    e.last_name,
    DATE_TRUNC('month', wh.date) as month,
    SUM(wh.hours) as total_hours,
    AVG(wh.hours) as avg_daily_hours
FROM employees e
JOIN work_hours wh ON e.id = wh.employee_id
GROUP BY e.id, e.first_name, e.last_name, DATE_TRUNC('month', wh.date);

-- Представление "Активные отпуска"
CREATE VIEW active_vacations AS
SELECT 
    e.first_name,
    e.last_name,
    v.type,
    v.start_date,
    v.end_date,
    (v.end_date - v.start_date) as duration_days
FROM vacations v
JOIN employees e ON v.employee_id = e.id
WHERE v.status = 'approved' 
AND v.start_date <= CURRENT_DATE 
AND v.end_date >= CURRENT_DATE;
