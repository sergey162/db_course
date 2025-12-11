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

CREATE TABLE work_hours (
    id SERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    hours DECIMAL(5,2) NOT NULL CHECK (hours >= 0 AND hours <= 24)
);

BEGIN;

INSERT INTO departments (name, description)
SELECT 'Department ' || i, 'Desc ' || i FROM generate_series(1, 50) i;

INSERT INTO positions (title, description, grade_level)
SELECT 'Pos ' || i, 'Desc ' || i, 'G' || (i % 5 + 1) FROM generate_series(1, 100) i;

-- 300,000 сотрудников
INSERT INTO employees (first_name, last_name, birth_date, email, phone, hire_date)
SELECT 
    'First' || i, 'Last' || i, 
    '1985-01-01'::date + (i % 10000), 
    'user' || i || '@example.com', 
    '+7900' || (1000000 + i),
    '2015-01-01'::date + (i % 2000)
FROM generate_series(1, 300000) i;

INSERT INTO assignments (employee_id, department_id, position_id, start_date)
SELECT id, (random()*49 + 1)::int, (random()*99 + 1)::int, hire_date
FROM employees;

-- 1.5 миллиона записей в work_hours
INSERT INTO work_hours (employee_id, date, hours)
SELECT 
    e.id, 
    '2023-01-01'::date + g.d, 
    (random() * 12)::numeric(5,2)
FROM employees e
CROSS JOIN generate_series(0, 5) as g(d)
WHERE e.id <= 250000;

COMMIT;
ANALYZE;