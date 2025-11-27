-- Лабораторная работа №4 - Тестовые данные

-- Очистка таблиц (в обратном порядке из-за foreign keys)
TRUNCATE TABLE work_hours, vacations, assignments, employees, departments, positions RESTART IDENTITY CASCADE;

-- Заполнение departments
INSERT INTO departments (name, description) VALUES 
('IT отдел', 'Отдел информационных технологий и разработки программного обеспечения'),
('HR отдел', 'Отдел кадров и управления персоналом'),
('Финансовый отдел', 'Финансовый отдел и бухгалтерия компании'),
('Отдел маркетинга', 'Отдел маркетинга и рекламных кампаний'),
('Отдел продаж', 'Отдел продаж и работы с клиентами');

-- Заполнение positions
INSERT INTO positions (title, description, grade_level) VALUES 
('Senior Developer', 'Старший разработчик программного обеспечения', 'L3'),
('Middle Developer', 'Разработчик', 'L2'),
('Junior Developer', 'Младший разработчик', 'L1'),
('HR Manager', 'Менеджер по персоналу', 'L2'),
('Finance Analyst', 'Финансовый аналитик', 'L2'),
('Marketing Specialist', 'Специалист по маркетингу', 'L1'),
('Sales Manager', 'Менеджер по продажам', 'L2'),
('Team Lead', 'Руководитель команды разработки', 'L4');

-- Заполнение employees
INSERT INTO employees (first_name, last_name, birth_date, email, phone, hire_date) VALUES 
('Иван', 'Иванов', '1985-05-15', 'ivanov@company.com', '+79990000001', '2020-01-15'),
('Петр', 'Петров', '1990-08-20', 'petrov@company.com', '+79990000002', '2022-03-10'),
('Мария', 'Сидорова', '1988-12-10', 'sidorova@company.com', '+79990000003', '2019-11-05'),
('Анна', 'Козлова', '1995-03-25', 'kozlova@company.com', '+79990000004', '2023-06-15'),
('Сергей', 'Смирнов', '1982-11-30', 'smirnov@company.com', '+79990000005', '2018-09-20'),
('Ольга', 'Новикова', '1992-07-14', 'novikova@company.com', '+79990000006', '2021-04-01'),
('Дмитрий', 'Васильев', '1987-04-18', 'vasiliev@company.com', '+79990000007', '2020-08-12'),
('Елена', 'Попова', '1993-09-22', 'popova@company.com', '+79990000008', '2022-11-30');

-- Заполнение assignments
INSERT INTO assignments (employee_id, department_id, position_id, start_date, end_date) VALUES 
(1, 1, 1, '2020-01-15', NULL),
(2, 1, 2, '2022-03-10', NULL),
(3, 2, 4, '2019-11-05', NULL),
(4, 3, 5, '2023-06-15', NULL),
(5, 1, 8, '2018-09-20', NULL),
(6, 4, 6, '2021-04-01', NULL),
(7, 5, 7, '2020-08-12', NULL),
(8, 2, 4, '2022-11-30', NULL);

-- Заполнение vacations
INSERT INTO vacations (employee_id, type, start_date, end_date, status) VALUES 
(1, 'ежегодный', '2023-07-01', '2023-07-14', 'approved'),
(2, 'ежегодный', '2023-08-15', '2023-08-29', 'pending'),
(3, 'больничный', '2023-06-10', '2023-06-20', 'approved'),
(4, 'ежегодный', '2023-09-01', '2023-09-14', 'approved');

-- Заполнение work_hours
INSERT INTO work_hours (employee_id, date, hours) VALUES 
(1, '2023-10-01', 8.0),
(1, '2023-10-02', 7.5),
(2, '2023-10-01', 8.0),
(2, '2023-10-02', 8.5),
(3, '2023-10-01', 8.0),
(4, '2023-10-01', 8.0);

-- Вывод информации о вставленных данных
SELECT 'Departments: ' || COUNT(*) FROM departments
UNION ALL
SELECT 'Positions: ' || COUNT(*) FROM positions
UNION ALL
SELECT 'Employees: ' || COUNT(*) FROM employees
UNION ALL
SELECT 'Assignments: ' || COUNT(*) FROM assignments
UNION ALL
SELECT 'Vacations: ' || COUNT(*) FROM vacations
UNION ALL
SELECT 'Work hours: ' || COUNT(*) FROM work_hours;
