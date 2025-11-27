-- Лабораторная работа №4 - Создание структуры БД

DROP TABLE IF EXISTS work_hours CASCADE;
DROP TABLE IF EXISTS vacations CASCADE;
DROP TABLE IF EXISTS assignments CASCADE;
DROP TABLE IF EXISTS employees CASCADE;
DROP TABLE IF EXISTS departments CASCADE;
DROP TABLE IF EXISTS positions CASCADE;

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

-- Комментарии к таблицам
COMMENT ON TABLE departments IS 'Таблица отделов компании';
COMMENT ON TABLE positions IS 'Таблица должностей';
COMMENT ON TABLE employees IS 'Таблица сотрудников';
COMMENT ON TABLE assignments IS 'Таблица назначений сотрудников по отделам и должностям';
COMMENT ON TABLE vacations IS 'Таблица отпусков сотрудников';
COMMENT ON TABLE work_hours IS 'Таблица отработанных часов';
