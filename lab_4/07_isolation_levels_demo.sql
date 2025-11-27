-- Лабораторная работа №4 - Очистка и завершение

-- Удаление индексов (в обратном порядке создания)
DROP INDEX IF EXISTS idx_work_hours_employee_date;
DROP INDEX IF EXISTS idx_vacations_employee_status;
DROP INDEX IF EXISTS idx_assignments_employee_dates;
DROP INDEX IF EXISTS idx_employees_name_composite;
DROP INDEX IF EXISTS idx_work_hours_employee_id;
DROP INDEX IF EXISTS idx_vacations_employee_id;
DROP INDEX IF EXISTS idx_assignments_position_id;
DROP INDEX IF EXISTS idx_assignments_department_id;
DROP INDEX IF EXISTS idx_assignments_employee_id;
DROP INDEX IF EXISTS idx_positions_description_trgm;
DROP INDEX IF EXISTS idx_departments_description_trgm;
DROP INDEX IF EXISTS idx_positions_title;
DROP INDEX IF EXISTS idx_departments_name;
DROP INDEX IF EXISTS idx_employees_first_name;
DROP INDEX IF EXISTS idx_employees_last_name;
DROP INDEX IF EXISTS idx_vacations_end_date;
DROP INDEX IF EXISTS idx_vacations_start_date;
DROP INDEX IF EXISTS idx_assignments_end_date;
DROP INDEX IF EXISTS idx_assignments_start_date;
DROP INDEX IF EXISTS idx_employees_hire_date;
DROP INDEX IF EXISTS idx_employees_birth_date;

-- Проверка удаления индексов
SELECT 
    tablename,
    indexname,
    indexdef
FROM pg_indexes 
WHERE schemaname = 'public'
ORDER BY tablename, indexname;

-- Финальный отчет
DO $$
DECLARE
    table_count integer;
    index_count integer;
BEGIN
    SELECT COUNT(*) INTO table_count 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_type = 'BASE TABLE';
    
    SELECT COUNT(*) INTO index_count 
    FROM pg_indexes 
    WHERE schemaname = 'public';
    
    RAISE NOTICE ' ';
    RAISE NOTICE '=== ЛАБОРАТОРНАЯ РАБОТА №4 ЗАВЕРШЕНА ===';
    RAISE NOTICE 'Количество таблиц: %', table_count;
    RAISE NOTICE 'Количество индексов: %', index_count;
    RAISE NOTICE ' ';
    RAISE NOTICE 'Выполнены все этапы:';
    RAISE NOTICE '1. Создание схемы БД и тестовых данных';
    RAISE NOTICE '2. Анализ и создание индексов';
    RAISE NOTICE '3. Тестирование производительности до/после индексов';
    RAISE NOTICE '4. Демонстрация транзакций и аномалий параллельного доступа';
    RAISE NOTICE '5. Исследование уровней изоляции транзакций';
    RAISE NOTICE ' ';
END $$;
