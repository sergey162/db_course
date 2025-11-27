# Лабораторная работа №4
## Индексы, транзакции и анализ производительности в PostgreSQL

### Структура файлов:

1. **`01_schema.sql`** - Создание структуры базы данных (6 таблиц)
2. **`02_sample_data.sql`** - Наполнение тестовыми данными
3. **`03_indexes_analysis.sql`** - Анализ запросов и создание индексов
4. **`04_performance_testing.sql`** - Тестирование производительности после индексов
5. **`05_transactions_demo.sql`** - Демонстрация аномалий транзакций
6. **`06_isolation_levels.sql`** - Исследование уровней изоляции
7. **`07_cleanup.sql`** - Очистка базы данных

### Порядок выполнения:

```bash
# 1. Создание структуры БД
psql -d your_database -f 01_schema.sql

# 2. Наполнение данными
psql -d your_database -f 02_sample_data.sql

# 3. Анализ и создание индексов
psql -d your_database -f 03_indexes_analysis.sql

# 4. Тестирование производительности
psql -d your_database -f 04_performance_testing.sql

# 5. Демонстрация транзакций (требует 2 параллельных сессии)
psql -d your_database -f 05_transactions_demo.sql

# 6. Исследование уровней изоляции
psql -d your_database -f 06_isolation_levels.sql

# 7. Очистка (опционально)
psql -d your_database -f 07_cleanup.sql
