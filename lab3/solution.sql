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

CREATE OR REPLACE FUNCTION check_vacation_availability(
    p_employee_id INTEGER,
    p_start_date DATE,
    p_end_date DATE
) RETURNS BOOLEAN AS $$
DECLARE
    total_vacation_days INTEGER;
    current_year_vacation_days INTEGER;
    new_vacation_days INTEGER;
BEGIN
    -- Проверка пересечения с существующими отпусками
    IF EXISTS (
        SELECT 1 FROM vacations 
        WHERE employee_id = p_employee_id 
        AND status = 'approved'
        AND (p_start_date, p_end_date) OVERLAPS (start_date, end_date)
    ) THEN
        RAISE EXCEPTION 'Отпуск пересекается с уже утвержденным отпуском';
    END IF;
    
    -- Расчет дней нового отпуска
    new_vacation_days := p_end_date - p_start_date + 1;
    
    -- Подсчет использованных дней отпуска за текущий год
    SELECT COALESCE(SUM(end_date - start_date + 1), 0)
    INTO current_year_vacation_days
    FROM vacations 
    WHERE employee_id = p_employee_id 
    AND status = 'approved'
    AND EXTRACT(YEAR FROM start_date) = EXTRACT(YEAR FROM CURRENT_DATE);
    
    -- Проверка лимита (28 дней в году)
    IF current_year_vacation_days + new_vacation_days > 28 THEN
        RAISE EXCEPTION 'Превышен лимит отпускных дней. Использовано: %, запрашивается: %', 
                        current_year_vacation_days, new_vacation_days;
    END IF;
    
    RETURN TRUE;
EXCEPTION
    WHEN unique_violation THEN
        RAISE EXCEPTION 'Нарушение уникальности данных';
    WHEN foreign_key_violation THEN
        RAISE EXCEPTION 'Сотрудник не существует';
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Ошибка при проверке отпуска: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE add_employee_with_assignment(
    p_first_name VARCHAR(50),
    p_last_name VARCHAR(50),
    p_birth_date DATE,
    p_email VARCHAR(100),
    p_phone VARCHAR(20),
    p_department_id INTEGER,
    p_position_id INTEGER
) AS $$
DECLARE
    v_employee_id INTEGER;
    v_min_age CONSTANT INTEGER := 18;
    v_current_age INTEGER;
BEGIN
    -- Проверка возраста
    v_current_age := EXTRACT(YEAR FROM AGE(p_birth_date));
    IF v_current_age < v_min_age THEN
        RAISE EXCEPTION 'Сотрудник должен быть старше % лет. Текущий возраст: %', 
                        v_min_age, v_current_age;
    END IF;
    
    -- Вставка сотрудника
    INSERT INTO employees (first_name, last_name, birth_date, email, phone, hire_date)
    VALUES (p_first_name, p_last_name, p_birth_date, p_email, p_phone, CURRENT_DATE)
    RETURNING id INTO v_employee_id;
    
    -- Создание назначения
    INSERT INTO assignments (employee_id, department_id, position_id, start_date)
    VALUES (v_employee_id, p_department_id, p_position_id, CURRENT_DATE);
    
    RAISE NOTICE 'Сотрудник % % успешно добавлен с ID: %', 
                 p_first_name, p_last_name, v_employee_id;
    
EXCEPTION
    WHEN unique_violation THEN
        RAISE EXCEPTION 'Email % уже существует', p_email;
    WHEN foreign_key_violation THEN
        RAISE EXCEPTION 'Неверный department_id или position_id';
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Ошибка при добавлении сотрудника: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION validate_vacation_dates()
RETURNS TRIGGER AS $$
BEGIN
    -- Проверка что дата начала не в прошлом
    IF NEW.start_date < CURRENT_DATE THEN
        RAISE EXCEPTION 'Дата начала отпуска не может быть в прошлом';
    END IF;
    
    -- Проверка что отпуск не более 30 дней
    IF (NEW.end_date - NEW.start_date) > 30 THEN
        RAISE EXCEPTION 'Отпуск не может быть более 30 дней';
    END IF;
    
    -- Автоматическая установка статуса
    IF NEW.status IS NULL THEN
        NEW.status := 'pending';
    END IF;
    
    RETURN NEW;
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Ошибка валидации отпуска: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_validate_vacation_dates
    BEFORE INSERT OR UPDATE ON vacations
    FOR EACH ROW
    EXECUTE FUNCTION validate_vacation_dates();

CREATE TABLE employee_audit (
    id SERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL,
    change_type VARCHAR(10) NOT NULL,
    change_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    old_data JSONB,
    new_data JSONB,
    changed_by VARCHAR(100)
);

CREATE OR REPLACE FUNCTION log_employee_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO employee_audit (employee_id, change_type, new_data, changed_by)
        VALUES (NEW.id, 'INSERT', row_to_json(NEW), current_user);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO employee_audit (employee_id, change_type, old_data, new_data, changed_by)
        VALUES (NEW.id, 'UPDATE', row_to_json(OLD), row_to_json(NEW), current_user);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO employee_audit (employee_id, change_type, old_data, changed_by)
        VALUES (OLD.id, 'DELETE', row_to_json(OLD), current_user);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_log_employee_changes
    AFTER INSERT OR UPDATE OR DELETE ON employees
    FOR EACH ROW
    EXECUTE FUNCTION log_employee_changes();

CREATE OR REPLACE FUNCTION validate_work_hours()
RETURNS TRIGGER AS $$
BEGIN
    -- Проверка что дата не в будущем
    IF NEW.date > CURRENT_DATE THEN
        RAISE EXCEPTION 'Нельзя добавлять рабочие часы на будущую дату';
    END IF;
    
    -- Проверка что сотрудник был трудоустроен на эту дату
    IF NOT EXISTS (
        SELECT 1 FROM assignments 
        WHERE employee_id = NEW.employee_id 
        AND start_date <= NEW.date 
        AND (end_date IS NULL OR end_date >= NEW.date)
    ) THEN
        RAISE EXCEPTION 'Сотрудник не был трудоустроен на дату %', NEW.date;
    END IF;
    
    RETURN NEW;
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Ошибка валидации рабочего времени: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_validate_work_hours
    BEFORE INSERT OR UPDATE ON work_hours
    FOR EACH ROW
    EXECUTE FUNCTION validate_work_hours();

-- Примеры использования
-- Добавление отделов и должностей
INSERT INTO departments (name, description) VALUES 
('IT', 'Отдел информационных технологий'),
('HR', 'Отдел кадров');

INSERT INTO positions (title, description, grade_level) VALUES 
('Software Developer', 'Разработчик программного обеспечения', 'Middle'),
('HR Manager', 'Менеджер по персоналу', 'Junior');

-- Добавление сотрудника через процедуру
CALL add_employee_with_assignment(
    'Иван', 'Петров', '1990-05-15', 
    'ivan.petrov@company.com', '+79161234567', 1, 1
);

-- Попытка добавления сотрудника с существующим email (вызовет ошибку)
CALL add_employee_with_assignment(
    'Петр', 'Иванов', '1985-03-20', 
    'ivan.petrov@company.com', '+79167654321', 1, 1
);
-- Проверка возможности взять отпуск
SELECT check_vacation_availability(1, '2024-07-01', '2024-07-14');

-- Создание отпуска (будет проверено триггером)
INSERT INTO vacations (employee_id, type, start_date, end_date) 
VALUES (1, 'ежегодный', '2024-07-01', '2024-07-14');

-- Попытка создать некорректный отпуск (вызовет ошибку)
INSERT INTO vacations (employee_id, type, start_date, end_date) 
VALUES (1, 'ежегодный', '2024-07-01', '2024-08-01'); -- более 30 дней

-- Добавление рабочих часов
INSERT INTO work_hours (employee_id, date, hours) VALUES
(1, '2024-06-01', 8),
(1, '2024-06-02', 7.5),
(1, '2024-06-03', 8);

-- Расчет зарплаты за месяц
SELECT calculate_salary(1, 6, 2024) as salary;

-- Попытка добавить рабочие часы на будущую дату (вызовет ошибку)
INSERT INTO work_hours (employee_id, date, hours) 
VALUES (1, '2025-01-01', 8);

-- Обновление данных сотрудника
UPDATE employees SET phone = '+79169998877' WHERE id = 1;

-- Просмотр аудита
SELECT * FROM employee_audit;
