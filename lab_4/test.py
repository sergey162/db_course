#!/usr/bin/env python3
"""
КОМПЛЕКСНОЕ ТЕСТИРОВАНИЕ ЛАБОРАТОРНОЙ РАБОТЫ №4
Индексы, транзакции и анализ производительности в PostgreSQL
"""

import psycopg2
import time
import threading
import sys
import random
from datetime import datetime

class Lab4CompleteTester:
    def __init__(self, dbname="lab4_test", user="postgres", password="password", host="localhost", port="5432"):
        self.db_params = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }
        self.connection = None
        self.results = []
        
    def connect(self):
        """Подключение к базе данных"""
        try:
            self.connection = psycopg2.connect(**self.db_params)
            self.connection.autocommit = False
            return True
        except Exception as e:
            print(f"✗ Ошибка подключения: {e}")
            return False
    
    def execute_sql(self, sql, params=None):
        """Выполнение SQL команды"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, params or ())
                if cursor.description:
                    return cursor.fetchall(), [desc[0] for desc in cursor.description]
                return None, None
        except Exception as e:
            print(f"✗ Ошибка SQL: {e}")
            self.connection.rollback()
            return None, None
    
    def execute_many(self, sql, params_list):
        """Выполнение множественных команд"""
        try:
            with self.connection.cursor() as cursor:
                cursor.executemany(sql, params_list)
            return True
        except Exception as e:
            print(f"✗ Ошибка executemany: {e}")
            self.connection.rollback()
            return False

    def create_schema(self):
        """Создание структуры базы данных"""
        print("\n" + "="*60)
        print("ЭТАП 1: СОЗДАНИЕ СТРУКТУРЫ БАЗЫ ДАННЫХ")
        print("="*60)
        
        schema_sql = """
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
        """
        
        success = self.execute_sql(schema_sql)[0] is not None
        if success:
            self.connection.commit()
            print("✓ Схема базы данных создана успешно")
            
            # Проверяем создание таблиц
            tables, _ = self.execute_sql("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name
            """)
            
            expected_tables = ['assignments', 'departments', 'employees', 'positions', 'vacations', 'work_hours']
            actual_tables = [table[0] for table in tables] if tables else []
            
            print(f"✓ Создано таблиц: {len(actual_tables)}")
            for table in actual_tables:
                print(f"  - {table}")
            
            if set(expected_tables).issubset(set(actual_tables)):
                self.results.append(("Создание схемы", "ПРОЙДЕН", f"{len(actual_tables)} таблиц"))
            else:
                self.results.append(("Создание схемы", "ОШИБКА", "Не все таблицы созданы"))
        else:
            self.results.append(("Создание схемы", "ОШИБКА", "Ошибка выполнения SQL"))
        
        return success

    def insert_sample_data(self):
        """Вставка тестовых данных"""
        print("\n" + "="*60)
        print("ЭТАП 2: ВСТАВКА ТЕСТОВЫХ ДАННЫХ")
        print("="*60)
        
        # Departments
        departments = [
            ('IT отдел', 'Отдел информационных технологий и разработки'),
            ('HR отдел', 'Отдел кадров и управления персоналом'),
            ('Финансовый отдел', 'Финансовый отдел и бухгалтерия компании'),
            ('Отдел маркетинга', 'Отдел маркетинга и рекламных кампаний')
        ]
        
        # Positions
        positions = [
            ('Senior Developer', 'Старший разработчик программного обеспечения', 'L3'),
            ('Middle Developer', 'Разработчик', 'L2'),
            ('HR Manager', 'Менеджер по персоналу', 'L2'),
            ('Finance Analyst', 'Финансовый аналитик', 'L2')
        ]
        
        # Employees
        employees = [
            ('Иван', 'Иванов', '1985-05-15', 'ivanov@company.com', '+79990000001', '2020-01-15'),
            ('Петр', 'Петров', '1990-08-20', 'petrov@company.com', '+79990000002', '2022-03-10'),
            ('Мария', 'Сидорова', '1988-12-10', 'sidorova@company.com', '+79990000003', '2019-11-05'),
            ('Анна', 'Козлова', '1995-03-25', 'kozlova@company.com', '+79990000004', '2023-06-15')
        ]
        
        # Assignments
        assignments = [
            (1, 1, 1, '2020-01-15', None),
            (2, 1, 2, '2022-03-10', None),
            (3, 2, 3, '2019-11-05', None),
            (4, 3, 4, '2023-06-15', None)
        ]
        
        # Vacations
        vacations = [
            (1, 'ежегодный', '2023-07-01', '2023-07-14', 'approved'),
            (2, 'ежегодный', '2023-08-15', '2023-08-29', 'pending')
        ]
        
        # Work hours
        work_hours = [
            (1, '2023-10-01', 8.0),
            (1, '2023-10-02', 7.5),
            (2, '2023-10-01', 8.0),
            (2, '2023-10-02', 8.5)
        ]
        
        try:
            # Вставляем данные
            self.execute_many("INSERT INTO departments (name, description) VALUES (%s, %s)", departments)
            self.execute_many("INSERT INTO positions (title, description, grade_level) VALUES (%s, %s, %s)", positions)
            self.execute_many("INSERT INTO employees (first_name, last_name, birth_date, email, phone, hire_date) VALUES (%s, %s, %s, %s, %s, %s)", employees)
            self.execute_many("INSERT INTO assignments (employee_id, department_id, position_id, start_date, end_date) VALUES (%s, %s, %s, %s, %s)", assignments)
            self.execute_many("INSERT INTO vacations (employee_id, type, start_date, end_date, status) VALUES (%s, %s, %s, %s, %s)", vacations)
            self.execute_many("INSERT INTO work_hours (employee_id, date, hours) VALUES (%s, %s, %s)", work_hours)
            
            self.connection.commit()
            
            # Проверяем вставленные данные
            tables = ['employees', 'departments', 'positions', 'assignments', 'vacations', 'work_hours']
            counts = {}
            
            for table in tables:
                result, _ = self.execute_sql(f"SELECT COUNT(*) FROM {table}")
                counts[table] = result[0][0] if result else 0
            
            print("✓ Данные успешно вставлены:")
            for table, count in counts.items():
                print(f"  - {table}: {count} записей")
            
            if all(count > 0 for count in counts.values()):
                self.results.append(("Вставка данных", "ПРОЙДЕН", f"{sum(counts.values())} записей"))
            else:
                self.results.append(("Вставка данных", "ОШИБКА", "Не все таблицы заполнены"))
                
            return True
            
        except Exception as e:
            print(f"✗ Ошибка вставки данных: {e}")
            self.connection.rollback()
            self.results.append(("Вставка данных", "ОШИБКА", str(e)))
            return False

    def analyze_before_indexes(self):
        """Анализ производительности ДО создания индексов"""
        print("\n" + "="*60)
        print("ЭТАП 3: АНАЛИЗ ПРОИЗВОДИТЕЛЬНОСТИ ДО ИНДЕКСОВ")
        print("="*60)
        
        test_queries = [
            ("Поиск по диапазону дат", 
             "SELECT * FROM employees WHERE birth_date BETWEEN '1980-01-01' AND '1990-12-31'"),
            
            ("Фильтрация по тексту", 
             "SELECT * FROM employees WHERE last_name LIKE 'Ива%' ORDER BY last_name, first_name"),
            
            ("JOIN запрос", 
             """SELECT e.first_name, e.last_name, d.name 
                FROM employees e 
                JOIN assignments a ON e.id = a.employee_id 
                JOIN departments d ON a.department_id = d.id 
                WHERE a.start_date >= '2020-01-01'""")
        ]
        
        print("Запросы ДО создания индексов:")
        performance_before = {}
        
        for test_name, query in test_queries:
            start_time = time.time()
            result, _ = self.execute_sql(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {query}")
            execution_time = time.time() - start_time
            
            if result:
                plan_text = '\n'.join([row[0] for row in result])
                uses_index = any(term in plan_text.lower() for term in ['index scan', 'bitmap index scan'])
                performance_before[test_name] = {
                    'time': execution_time,
                    'uses_index': uses_index,
                    'plan': plan_text
                }
                
                status = "ИСПОЛЬЗУЕТ ИНДЕКС" if uses_index else "ПОЛНОЕ СКАНИРОВАНИЕ"
                print(f"  {test_name}: {status} ({execution_time:.3f} сек)")
        
        self.results.append(("Анализ до индексов", "ПРОЙДЕН", f"{len(test_queries)} запросов"))
        return performance_before

    def create_indexes(self):
        """Создание индексов"""
        print("\n" + "="*60)
        print("ЭТАП 4: СОЗДАНИЕ ИНДЕКСОВ")
        print("="*60)
        
        indexes_sql = [
            # Индексы для поиска по диапазону значений
            "CREATE INDEX idx_employees_birth_date ON employees(birth_date)",
            "CREATE INDEX idx_employees_hire_date ON employees(hire_date)",
            "CREATE INDEX idx_assignments_start_date ON assignments(start_date)",
            
            # Индексы для текстовых полей
            "CREATE INDEX idx_employees_last_name ON employees(last_name)",
            "CREATE INDEX idx_employees_first_name ON employees(first_name)",
            "CREATE INDEX idx_departments_name ON departments(name)",
            
            # Индексы для внешних ключей
            "CREATE INDEX idx_assignments_employee_id ON assignments(employee_id)",
            "CREATE INDEX idx_assignments_department_id ON assignments(department_id)",
            "CREATE INDEX idx_assignments_position_id ON assignments(position_id)",
            "CREATE INDEX idx_vacations_employee_id ON vacations(employee_id)",
            
            # Составные индексы
            "CREATE INDEX idx_employees_name_composite ON employees(last_name, first_name)",
            "CREATE INDEX idx_assignments_employee_dates ON assignments(employee_id, start_date, end_date)"
        ]
        
        created_indexes = 0
        for sql in indexes_sql:
            if self.execute_sql(sql)[0] is not None:
                created_indexes += 1
        
        self.connection.commit()
        
        # Проверяем созданные индексы
        indexes, _ = self.execute_sql("""
            SELECT COUNT(*) 
            FROM pg_indexes 
            WHERE schemaname = 'public'
        """)
        
        index_count = indexes[0][0] if indexes else 0
        
        print(f"✓ Создано индексов: {created_indexes}")
        print(f"✓ Всего индексов в БД: {index_count}")
        
        self.results.append(("Создание индексов", "ПРОЙДЕН", f"{created_indexes} индексов"))
        return created_indexes > 0

    def analyze_after_indexes(self, performance_before):
        """Анализ производительности ПОСЛЕ создания индексов"""
        print("\n" + "="*60)
        print("ЭТАП 5: АНАЛИЗ ПРОИЗВОДИТЕЛЬНОСТИ ПОСЛЕ ИНДЕКСОВ")
        print("="*60)
        
        test_queries = [
            ("Поиск по диапазону дат", 
             "SELECT * FROM employees WHERE birth_date BETWEEN '1980-01-01' AND '1990-12-31'"),
            
            ("Фильтрация по тексту", 
             "SELECT * FROM employees WHERE last_name LIKE 'Ива%' ORDER BY last_name, first_name"),
            
            ("JOIN запрос", 
             """SELECT e.first_name, e.last_name, d.name 
                FROM employees e 
                JOIN assignments a ON e.id = a.employee_id 
                JOIN departments d ON a.department_id = d.id 
                WHERE a.start_date >= '2020-01-01'""")
        ]
        
        print("Запросы ПОСЛЕ создания индексов:")
        improvements = []
        
        for test_name, query in test_queries:
            start_time = time.time()
            result, _ = self.execute_sql(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {query}")
            execution_time = time.time() - start_time
            
            if result:
                plan_text = '\n'.join([row[0] for row in result])
                uses_index = any(term in plan_text.lower() for term in ['index scan', 'bitmap index scan'])
                
                before_time = performance_before.get(test_name, {}).get('time', execution_time)
                improvement = ((before_time - execution_time) / before_time) * 100
                
                status = "ИСПОЛЬЗУЕТ ИНДЕКС" if uses_index else "ПОЛНОЕ СКАНИРОВАНИЕ"
                print(f"  {test_name}: {status} ({execution_time:.3f} сек) - улучшение: {improvement:+.1f}%")
                
                improvements.append(improvement)
        
        avg_improvement = sum(improvements) / len(improvements) if improvements else 0
        self.results.append(("Анализ после индексов", "ПРОЙДЕН", f"улучшение: {avg_improvement:+.1f}%"))
        
        return avg_improvement

    def test_complex_queries(self):
        """Тестирование сложных запросов"""
        print("\n" + "="*60)
        print("ЭТАП 6: ТЕСТИРОВАНИЕ СЛОЖНЫХ ЗАПРОСОВ")
        print("="*60)
        
        complex_queries = [
            ("Агрегация с JOIN",
             """SELECT d.name, COUNT(DISTINCT a.employee_id) as emp_count
                FROM departments d
                LEFT JOIN assignments a ON d.id = a.department_id
                GROUP BY d.id, d.name
                ORDER BY emp_count DESC"""),
                
            ("Подзапросы и агрегация",
             """SELECT e.first_name, e.last_name,
                   (SELECT COUNT(*) FROM vacations v 
                    WHERE v.employee_id = e.id) as vacation_count
                FROM employees e
                WHERE e.id IN (SELECT employee_id FROM assignments)
                ORDER BY vacation_count DESC"""),
                
            ("Многотабличный JOIN",
             """SELECT e.first_name, e.last_name, d.name, p.title
                FROM employees e
                JOIN assignments a ON e.id = a.employee_id
                JOIN departments d ON a.department_id = d.id
                JOIN positions p ON a.position_id = p.id
                WHERE a.end_date IS NULL
                ORDER BY e.hire_date DESC""")
        ]
        
        successful_queries = 0
        for test_name, query in complex_queries:
            start_time = time.time()
            result, columns = self.execute_sql(query)
            execution_time = time.time() - start_time
            
            if result:
                print(f"✓ {test_name}: {len(result)} строк ({execution_time:.3f} сек)")
                successful_queries += 1
            else:
                print(f"✗ {test_name}: ОШИБКА")
        
        self.results.append(("Сложные запросы", "ПРОЙДЕН" if successful_queries == len(complex_queries) else "ОШИБКА", 
                           f"{successful_queries}/{len(complex_queries)} успешно"))

    def test_transactions_isolation(self):
        """Тестирование уровней изоляции транзакций"""
        print("\n" + "="*60)
        print("ЭТАП 7: ТЕСТИРОВАНИЕ ТРАНЗАКЦИЙ И УРОВНЕЙ ИЗОЛЯЦИИ")
        print("="*60)
        
        print("Этот этап требует ручного тестирования в двух параллельных сессиях.")
        print("\nИНСТРУКЦИЯ ДЛЯ РУЧНОГО ТЕСТИРОВАНИЯ:")
        print("1. Откройте ДВА окна терминала с подключением к базе данных:")
        print("   psql -d lab4_test")
        print("2. В каждом окне выполняйте команды из следующих сценариев:")
        
        scenarios = {
            "Dirty Read": [
                "-- Окно 1:",
                "BEGIN;",
                "UPDATE employees SET first_name = 'Dirty_Test' WHERE id = 1;",
                "-- Не коммитить!",
                "",
                "-- Окно 2:",
                "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
                "BEGIN;",
                "SELECT first_name FROM employees WHERE id = 1;",
                "COMMIT;",
                "",
                "-- Окно 1:",
                "ROLLBACK;"
            ],
            "Non-repeatable Read": [
                "-- Окно 1:",
                "BEGIN;",
                "SELECT first_name FROM employees WHERE id = 2;",
                "-- Ждем...",
                "SELECT first_name FROM employees WHERE id = 2;",
                "COMMIT;",
                "",
                "-- Окно 2 (между чтениями):",
                "BEGIN;",
                "UPDATE employees SET first_name = 'Changed' WHERE id = 2;",
                "COMMIT;"
            ]
        }
        
        for scenario_name, commands in scenarios.items():
            print(f"\n--- {scenario_name} ---")
            for cmd in commands:
                print(f"  {cmd}")
        
        self.results.append(("Тестирование транзакций", "ТРЕБУЕТ РУЧНОГО ТЕСТИРОВАНИЯ", "см. инструкцию выше"))

    def run_performance_benchmark(self):
        """Запуск производительного тестирования"""
        print("\n" + "="*60)
        print("ЭТАП 8: ПРОИЗВОДИТЕЛЬНОЕ ТЕСТИРОВАНИЕ")
        print("="*60)
        
        # Тестируем различные типы запросов
        benchmark_queries = [
            ("Точечный поиск", "SELECT * FROM employees WHERE id = 1"),
            ("Диапазонный поиск", "SELECT * FROM employees WHERE birth_date BETWEEN '1985-01-01' AND '1995-12-31'"),
            ("Текстовый поиск", "SELECT * FROM employees WHERE last_name LIKE 'Ива%'"),
            ("Сортировка", "SELECT * FROM employees ORDER BY hire_date DESC LIMIT 10"),
            ("Агрегация", "SELECT department_id, COUNT(*) FROM assignments GROUP BY department_id")
        ]
        
        total_time = 0
        successful_queries = 0
        
        for query_name, query in benchmark_queries:
            times = []
            for _ in range(3):  # Запускаем 3 раза для усреднения
                start_time = time.time()
                result, _ = self.execute_sql(query)
                end_time = time.time()
                if result is not None:
                    times.append(end_time - start_time)
            
            if times:
                avg_time = sum(times) / len(times)
                total_time += avg_time
                successful_queries += 1
                print(f"✓ {query_name}: {avg_time:.4f} сек")
            else:
                print(f"✗ {query_name}: ОШИБКА")
        
        if successful_queries > 0:
            avg_total_time = total_time / successful_queries
            self.results.append(("Производительное тестирование", "ПРОЙДЕН", 
                               f"среднее время: {avg_total_time:.4f} сек"))
        else:
            self.results.append(("Производительное тестирование", "ОШИБКА", "все запросы завершились ошибкой"))

    def generate_report(self):
        """Генерация итогового отчета"""
        print("\n" + "="*60)
        print("ИТОГОВЫЙ ОТЧЕТ")
        print("="*60)
        
        passed = sum(1 for _, status, _ in self.results if "ПРОЙДЕН" in status or "РУЧНОГО" in status)
        total = len(self.results)
        
        print("\nРезультаты по этапам:")
        for i, (test_name, status, details) in enumerate(self.results, 1):
            icon = "✓" if "ПРОЙДЕН" in status else "⟳" if "РУЧНОГО" in status else "✗"
            print(f"{i:2d}. {icon} {test_name}: {status}")
            if details:
                print(f"      {details}")
        
        print(f"\n📊 ИТОГО: {passed}/{total} этапов успешно")
        
        if passed == total:
            print("🎉 Все основные тесты пройдены успешно!")
        else:
            print("⚠ Требуется дополнительная проверка некоторых этапов")
        
        print("\nДальнейшие действия:")
        print("1. Для тестирования транзакций выполните ручное тестирование по инструкции")
        print("2. Проверьте логи выполнения запросов")
        print("3. Проанализируйте использование индексов через pg_stat_user_indexes")

    def cleanup(self):
        """Очистка ресурсов"""
        if self.connection:
            self.connection.close()

    def run_complete_test(self):
        """Запуск полного тестирования"""
        print("🚀 КОМПЛЕКСНОЕ ТЕСТИРОВАНИЕ ЛАБОРАТОРНОЙ РАБОТЫ №4")
        print("База данных:", self.db_params['dbname'])
        print("Время начала:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print("=" * 70)
        
        try:
            # Создаем базу данных если нужно
            try:
                conn_default = psycopg2.connect(**{**self.db_params, 'dbname': 'postgres'})
                conn_default.autocommit = True
                with conn_default.cursor() as cursor:
                    cursor.execute(f"DROP DATABASE IF EXISTS {self.db_params['dbname']}")
                    cursor.execute(f"CREATE DATABASE {self.db_params['dbparams['dbname']}")
                conn_default.close()
                print("✓ Тестовая база данных создана")
            except Exception as e:
                print(f"⚠ Используем существующую базу данных: {e}")
            
            if not self.connect():
                return False
            
            # Выполняем все этапы тестирования
            self.create_schema()
            self.insert_sample_data()
            
            performance_before = self.analyze_before_indexes()
            self.create_indexes()
            self.analyze_after_indexes(performance_before)
            
            self.test_complex_queries()
            self.run_performance_benchmark()
            self.test_transactions_isolation()
            
            self.generate_report()
            return True
            
        except Exception as e:
            print(f"💥 Критическая ошибка: {e}")
            return False
        finally:
            self.cleanup()

def main():
    """Основная функция"""
    if len(sys.argv) > 1:
        dbname = sys.argv[1]
    else:
        dbname = "lab4_test"
    
    tester = Lab4CompleteTester(dbname=dbname)
    
    # Проверяем подключение к PostgreSQL
    try:
        conn = psycopg2.connect(**{**tester.db_params, 'dbname': 'postgres'})
        conn.close()
    except Exception as e:
        print(f"❌ Не удается подключиться к PostgreSQL: {e}")
        print("Убедитесь, что:")
        print("1. PostgreSQL запущен")
        print("2. Правильные параметры подключения в коде")
        print("3. Установлен psycopg2: pip install psycopg2-binary")
        return
    
    success = tester.run_complete_test()
    
    if success:
        print(f"\n✅ Тестирование завершено успешно!")
    else:
        print(f"\n❌ Тестирование завершено с ошибками!")
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
