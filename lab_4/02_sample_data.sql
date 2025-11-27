-- Лабораторная работа №4 - Тестовые данные

TRUNCATE TABLE assignments, vacations, work_hours, employees, departments, positions RESTART IDENTITY CASCADE;

INSERT INTO departments (name, description) VALUES 
('IT отдел', 'Отдел информационных технологий и разработки'),
('HR отдел', 'Отдел кадров и управления персоналом'),
('Финансы', 'Финансовый отдел и бухгалтерия'),
('Маркетинг', 'Отдел маркетинга и рекламы');

INSERT INTO positions (title, description, grade_level) VALUES 
('Senior Developer', 'Старший разработчик', 'L3'),
('Middle Developer', 'Разработчик', 'L2'),
('HR Manager', 'Менеджер по персоналу', 'L2'),
('Finance Analyst', 'Финансовый аналитик', 'L2'),
('Marketing Specialist', 'Специалист по маркетингу', 'L1');

INSERT INTO employees (first_name, last_name, birth_date, email, phone, hire_date) VALUES 
('Иван', 'Иванов', '1985-05-15', 'ivanov@company.com', '+79990000001', '2020-01-15'),
('Петр', 'Петров', '1990-08-20', 'petrov@company.com', '+79990000002', '2022-03-10'),
('Мария', 'Сидорова', '1988-12-10', 'sidorova@company.com', '+79990000003', '2019-11-05'),
('Анна', 'Козлова', '1995-03-25', 'kozlova@company.com', '+79990000004', '2023-06-15');
