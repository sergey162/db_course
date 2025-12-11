import psycopg2
import time
import threading

DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'new_password',
    'host': 'localhost',
    'port': '5432'
}

print_lock = threading.Lock()

def log(msg):
    with print_lock:
        print(msg)

def get_conn(isolation_level=None):
    conn = psycopg2.connect(**DB_CONFIG)
    if isolation_level is not None:
        conn.set_isolation_level(isolation_level)
    else:
        conn.autocommit = True
    return conn

def read_sql_file(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        return f.read()

def run_explain(cursor, query, step_name):
    log(f"   [{step_name}] Замер...")
    cursor.execute("EXPLAIN ANALYZE " + query)
    rows = cursor.fetchall()
    exec_time = "0"
    plan_node = "Unknown"
    
    for row in rows:
        line = row[0]
        if "Seq Scan" in line and plan_node == "Unknown": plan_node = "Seq Scan (Full)"
        if "Index Scan" in line and plan_node == "Unknown": plan_node = "Index Scan (Fast)"
        if "Bitmap Heap Scan" in line and plan_node == "Unknown": plan_node = "Bitmap Scan"
        if "Nested Loop" in line: plan_node += " + Nested Loop"
        if "Hash Join" in line: plan_node += " + Hash Join"
        
        if "Execution Time" in line:
            exec_time = line.split(":")[1].strip().replace(" ms", "")
            
    log(f"     -> Стратегия: {plan_node}")
    log(f"     -> Время: {exec_time} ms")
    return float(exec_time)

def task_indexes():
    log("\nЗАДАНИЕ 1 и 2: ИНДЕКСЫ")
    conn = get_conn()
    cur = conn.cursor()

    log("Генерация данных")
    cur.execute(read_sql_file("01_init_db.sql")) 
    log("База готова.")
    
    log("\n[Тест 1] Поиск редкого значения (WHERE hours = 8.5 AND date = ...)")
    q1 = "SELECT * FROM work_hours WHERE date = '2023-01-03' AND hours = 8.5"
    
    t1 = run_explain(cur, q1, "ДО индексов")
    
    cur.execute("CREATE INDEX idx_wh_composite ON work_hours(date, hours);")
    
    cur.execute("SET enable_seqscan = OFF;") 
    t2 = run_explain(cur, q1, "ПОСЛЕ индексов")
    cur.execute("SET enable_seqscan = ON;")
    
    if t2 > 0:
        log(f"     >>> УСКОРЕНИЕ: в {t1/t2:.1f} раз")

    log("\n[Тест 2] JOIN 4-х таблиц (Фильтр по Департаменту)")
    q_complex = """
    SELECT d.name, COUNT(e.id), SUM(wh.hours)
    FROM departments d
    JOIN assignments a ON d.id = a.department_id
    JOIN employees e ON a.employee_id = e.id
    JOIN work_hours wh ON e.id = wh.employee_id
    WHERE d.name = 'Department 10' 
    GROUP BY d.name
    """
    
    t3 = run_explain(cur, q_complex, "ДО индексов")
    
    cur.execute("CREATE INDEX idx_dept_name ON departments(name);")
    cur.execute("CREATE INDEX idx_assign_dept ON assignments(department_id);")
    cur.execute("CREATE INDEX idx_assign_emp ON assignments(employee_id);")
    cur.execute("CREATE INDEX idx_wh_emp ON work_hours(employee_id);")
    
    cur.execute("SET enable_seqscan = OFF;")
    t4 = run_explain(cur, q_complex, "ПОСЛЕ индексов")
    cur.execute("SET enable_seqscan = ON;")
    
    if t4 > 0:
        log(f"     >>> УСКОРЕНИЕ: в {t3/t4:.1f} раз")
    
    conn.close()


def scenario_1_non_repeatable():
    log("\n=== СЦЕНАРИЙ 1: Non-repeatable Read ===")
    c = get_conn()
    cur = c.cursor()
    cur.execute("UPDATE work_hours SET hours = 10.0 WHERE id = 1")
    c.close()

    def t1_func():
        try:
            conn = get_conn(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
            cur = conn.cursor()
            cur.execute("SELECT hours FROM work_hours WHERE id = 1")
            val1 = cur.fetchone()[0]
            log(f"[T1] Чтение 1: {val1}")
            time.sleep(2)
            cur.execute("SELECT hours FROM work_hours WHERE id = 1")
            val2 = cur.fetchone()[0]
            log(f"[T1] Чтение 2: {val2}")
            if val1 != val2:
                log(">>> АНОМАЛИЯ ПОЙМАНА (значения разные).")
            conn.commit()
            conn.close()
        except Exception as e: log(e)

    def t2_func():
        try:
            time.sleep(0.5)
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("UPDATE work_hours SET hours = 20.0 WHERE id = 1")
            log("[T2] Данные обновлены.")
            conn.close()
        except Exception as e: log(e)

    t1 = threading.Thread(target=t1_func); t2 = threading.Thread(target=t2_func)
    t1.start(); t2.start(); t1.join(); t2.join()

def scenario_2_phantom():
    log("\n=== СЦЕНАРИЙ 2: Phantom Read ===")
    c = get_conn()
    cur = c.cursor()
    cur.execute("DELETE FROM departments WHERE name = 'Phantom Dept'")
    c.close()

    def t1_func():
        try:
            conn = get_conn(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM departments")
            c1 = cur.fetchone()[0]
            log(f"[T1] Кол-во строк: {c1}")
            time.sleep(2)
            cur.execute("SELECT COUNT(*) FROM departments")
            c2 = cur.fetchone()[0]
            log(f"[T1] Кол-во строк: {c2}")
            if c1 != c2:
                log(">>> АНОМАЛИЯ ПОЙМАНА (появился фантом).")
            conn.commit()
            conn.close()
        except Exception as e: log(e)

    def t2_func():
        try:
            time.sleep(0.5)
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("INSERT INTO departments (name, description) VALUES ('Phantom Dept', 'Test')")
            log("[T2] Вставили строку.")
            conn.close()
        except Exception as e: log(e)

    t1 = threading.Thread(target=t1_func); t2 = threading.Thread(target=t2_func)
    t1.start(); t2.start(); t1.join(); t2.join()

def scenario_3_dirty_read():
    log("\n=== СЦЕНАРИЙ 3: Dirty Read ===")
    c = get_conn()
    cur = c.cursor()
    cur.execute("UPDATE work_hours SET hours = 10.0 WHERE id = 1")
    c.close()

    def t2_writer():
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = False
            cur = conn.cursor()
            cur.execute("UPDATE work_hours SET hours = 23.0 WHERE id = 1")
            log("[T2] Update БЕЗ commit (ждем)...")
            time.sleep(3)
            conn.rollback()
            log("[T2] Rollback.")
            conn.close()
        except Exception as e: log(f"Ошибка T2: {e}")

    def t1_reader():
        try:
            time.sleep(1)
            conn = get_conn(psycopg2.extensions.ISOLATION_LEVEL_READ_UNCOMMITTED)
            cur = conn.cursor()
            cur.execute("SELECT hours FROM work_hours WHERE id = 1")
            val = cur.fetchone()[0]
            log(f"[T1] Прочитано: {val}")
            if val == 23.0:
                log("грязное чтение!")
            else:
                log("Грязное чтение предотвращено.")
            conn.commit()
            conn.close()
        except Exception as e: log(e)

    t2 = threading.Thread(target=t2_writer); t1 = threading.Thread(target=t1_reader)
    t2.start(); t1.start(); t2.join(); t1.join()


task_indexes()
scenario_1_non_repeatable()
scenario_2_phantom()
scenario_3_dirty_read()