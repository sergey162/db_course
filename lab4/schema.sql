-- 1. Удаляем таблицы, если они существуют (для чистого старта)
DROP TABLE IF EXISTS work_hours CASCADE;
DROP TABLE IF EXISTS vacations CASCADE;
DROP TABLE IF EXISTS assignments CASCADE;
DROP TABLE IF EXISTS employees CASCADE;
DROP TABLE IF EXISTS positions CASCADE;
DROP TABLE IF EXISTS departments CASCADE;

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

BEGIN;

-- Вставляем 20 департаментов
INSERT INTO departments (name, description)
SELECT 'Department ' || i, 'Description for department ' || i
FROM generate_series(1, 20) i;

-- Вставляем 50 должностей
INSERT INTO positions (title, description, grade_level)
SELECT 'Position ' || i, 'Description ' || i, 'Grade ' || (i % 5 + 1)
FROM generate_series(1, 50) i;

-- Вставляем 100,000 сотрудников
INSERT INTO employees (first_name, last_name, birth_date, email, phone, hire_date)
SELECT 
    'First' || i, 
    'Last' || i, 
    '1980-01-01'::date + (random() * 10000)::int, 
    'user' || i || '@example.com', 
    '+7900' || (1000000 + i),
    '2015-01-01'::date + (random() * 2000)::int
FROM generate_series(1, 100000) i;

INSERT INTO assignments (employee_id, department_id, position_id, start_date)
SELECT 
    id, 
    (random() * 19 + 1)::int, 
    (random() * 49 + 1)::int, 
    hire_date
FROM employees;

-- Генерируем рабочие часы (по 5 записей на каждого из первых 50,000 сотрудников = 250,000 записей)
INSERT INTO work_hours (employee_id, date, hours)
SELECT 
    e.id, 
    '2023-01-01'::date + g.d, 
    4 + (random() * 8)::int -- случайные часы от 4 до 12
FROM employees e
CROSS JOIN generate_series(0, 4) as g(d)
WHERE e.id <= 50000;

COMMIT;

ANALYZE; -- Обновляем статистику для планировщика
