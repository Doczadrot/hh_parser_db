# db.py - Файл с функциями для работы с базой данных

import psycopg2
from dotenv import load_dotenv
import os
import psycopg2.errors

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

db_params = {
    "database": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST,
    "port": DB_PORT
}


def create_database(params):
    """
    Создает базу данных, если она не существует.
    
    Args:
        params (dict): Параметры подключения к PostgreSQL.
    """
    # Подключаемся к postgres для создания новой БД
    conn_params = params.copy()
    conn_params['database'] = 'postgres'  # Временно подключаемся к postgres
    
    conn = None
    cur = None
    db_name = params['database']
    
    try:
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True  # Автоматический коммит для создания БД
        cur = conn.cursor()
        
        # Проверяем, существует ли база данных
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        exists = cur.fetchone()
        
        if not exists:
            print(f"🔧 Создаем базу данных {db_name}...")
            # Создаем базу данных
            cur.execute(f"CREATE DATABASE {db_name}")
            print(f"✅ База данных {db_name} успешно создана!")
        else:
            print(f"ℹ️ База данных {db_name} уже существует.")
            
    except Exception as e:
        print(f"❌ Ошибка при создании базы данных: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def create_tables(params):
    """
    Создает таблицы в базе данных, если они не существуют.
    
    Args:
        params (dict): Параметры подключения к PostgreSQL.
    """
    conn = None
    cur = None
    
    try:
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        
        # Создаем таблицу employers (работодатели)
        create_employers_table = """
        CREATE TABLE IF NOT EXISTS employers (
            employer_id VARCHAR(50) PRIMARY KEY,
            employer_name VARCHAR(255) NOT NULL,
            employer_url VARCHAR(255),
            open_vacancies INTEGER
        );
        """
        
        # Создаем таблицу vacancies (вакансии)
        create_vacancies_table = """
        CREATE TABLE IF NOT EXISTS vacancies (
            vacancy_id VARCHAR(50) PRIMARY KEY,
            employer_id VARCHAR(50) REFERENCES employers(employer_id),
            title VARCHAR(255) NOT NULL,
            salary_from INTEGER,
            salary_to INTEGER,
            salary_currency VARCHAR(10),
            url VARCHAR(255)
        );
        """
        
        # Выполняем SQL-запросы для создания таблиц
        cur.execute(create_employers_table)
        cur.execute(create_vacancies_table)
        
        # Фиксируем изменения
        conn.commit()
        print("✅ Таблицы 'employers' и 'vacancies' успешно созданы (или уже существуют)!")
        
    except Exception as e:
        print(f"❌ Ошибка при создании таблиц: {e}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


class DBManager:
    """
    Класс для выполнения операций выборки данных из базы данных PostgreSQL.
    """

    def __init__(self, params):
        """
        Инициализирует менеджер базы данных с параметрами подключения.

        Args:
            params (dict): Параметры подключения к БД.
        """
        self.params = params

    def get_companies_and_vacancies_count(self):
        """
        Получает список всех компаний и количество открытых вакансий у каждой.

        Returns:
            list: Список кортежей (название компании, количество вакансий).
        """
        conn = None
        cur = None
        results = []

        try:
            conn = psycopg2.connect(**self.params)
            cur = conn.cursor()

            sql_query = """
                SELECT e.employer_name, COUNT(v.vacancy_id)
                FROM employers e
                LEFT JOIN vacancies v ON e.employer_id = v.employer_id
                GROUP BY e.employer_name
                ORDER BY COUNT(v.vacancy_id) DESC;
            """
            cur.execute(sql_query)
            results = cur.fetchall()

        except Exception as e:
            print(f"❌ Ошибка при выполнении запроса 'get_companies_and_vacancies_count': {e}")
            results = []

        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

        return results

    def get_all_vacancies(self):
        """
        Получает список всех вакансий из БД с указанием названия компании, названия вакансии,
        зарплаты и ссылки на вакансию.

        Returns:
            list: Список кортежей вакансий с информацией о компании.
        """
        conn = None
        cur = None
        results = []

        try:
            conn = psycopg2.connect(**self.params)
            cur = conn.cursor()

            sql_query = """
                SELECT v.vacancy_id, e.employer_name, v.title, v.salary_from, v.salary_to, 
                       v.salary_currency, v.url
                FROM vacancies v
                JOIN employers e ON v.employer_id = e.employer_id;
            """
            cur.execute(sql_query)
            results = cur.fetchall()

        except Exception as e:
            print(f"❌ Ошибка при выполнении запроса 'get_all_vacancies': {e}")
            results = []

        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

        return results

    def get_vacancies_with_salary(self):
        """
        Получает список вакансий с указанной зарплатой.

        Returns:
            list: Список кортежей вакансий с зарплатой.
        """
        conn = None
        cur = None
        results = []

        try:
            conn = psycopg2.connect(**self.params)
            cur = conn.cursor()

            sql_query = """
                SELECT *
                FROM vacancies
                WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL;
            """
            cur.execute(sql_query)
            results = cur.fetchall()

        except Exception as e:
            print(f"❌ Ошибка при выполнении запроса 'get_vacancies_with_salary': {e}")
            results = []

        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

        return results

    def get_vacancies_with_keyword(self, keyword):
        """
        Получает список всех вакансий, в названии которых содержится ключевое слово.

        Args:
            keyword (str): Ключевое слово для поиска в названии вакансии.

        Returns:
            list: Список кортежей вакансий, содержащих ключевое слово.
        """
        conn = None
        cur = None
        results = []

        try:
            conn = psycopg2.connect(**self.params)
            cur = conn.cursor()

            # SQL-запрос для получения вакансий по ключевому слову в названии
            # ILIKE %s - ищет подстроку без учета регистра. %s - плейсхолдер.
            sql_query = """
                SELECT *
                FROM vacancies
                WHERE title ILIKE %s;
            """
            # Формируем ключевое слово для поиска с символами % вокруг него
            # Это нужно для поиска подстроки в любом месте заголовка
            search_keyword = f"%{keyword}%"

            # Выполняем запрос, передавая SQL-строку и КОРТЕЖ со значением плейсхолдера
            cur.execute(sql_query, (search_keyword,)) # <-- **ВАЖНО!** Передаем (search_keyword,) как кортеж!

            results = cur.fetchall()

        except Exception as e:
            print(f"❌ Ошибка при выполнении запроса 'get_vacancies_with_keyword': {e}")
            results = []

        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

        return results
        
    def get_avg_salary(self):
        """
        Получает среднюю зарплату по всем вакансиям.
        Учитывает как нижнюю (salary_from), так и верхнюю (salary_to) границы зарплаты.

        Returns:
            float: Средняя зарплата по всем вакансиям или None, если нет данных.
        """
        conn = None
        cur = None
        avg_salary = None

        try:
            conn = psycopg2.connect(**self.params)
            cur = conn.cursor()

            # SQL-запрос для получения средней зарплаты
            # COALESCE используется для замены NULL на 0
            # NULLIF используется для предотвращения деления на 0
            sql_query = """
                SELECT AVG(COALESCE(salary_from, 0) + COALESCE(salary_to, 0)) / 
                       NULLIF(CASE 
                           WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL THEN 2
                           WHEN salary_from IS NOT NULL OR salary_to IS NOT NULL THEN 1
                           ELSE 0
                       END, 0) as avg_salary
                FROM vacancies
                WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL;
            """
            cur.execute(sql_query)
            avg_salary = cur.fetchone()[0]  # Получаем первый элемент первой строки результата

        except Exception as e:
            print(f"❌ Ошибка при выполнении запроса 'get_avg_salary': {e}")
            avg_salary = None

        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

        return avg_salary

    def get_vacancies_with_higher_salary(self):
        """
        Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.

        Returns:
            list: Список кортежей вакансий с зарплатой выше средней.
        """
        conn = None
        cur = None
        results = []

        try:
            conn = psycopg2.connect(**self.params)
            cur = conn.cursor()

            # SQL-запрос для получения вакансий с зарплатой выше средней
            # Используем подзапрос для вычисления средней зарплаты
            sql_query = """
                WITH avg_salary AS (
                    SELECT AVG(COALESCE(salary_from, 0) + COALESCE(salary_to, 0)) / 
                           NULLIF(CASE 
                               WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL THEN 2
                               WHEN salary_from IS NOT NULL OR salary_to IS NOT NULL THEN 1
                               ELSE 0
                           END, 0) as avg_value
                    FROM vacancies
                    WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL
                )
                SELECT v.*, e.employer_name
                FROM vacancies v
                JOIN employers e ON v.employer_id = e.employer_id
                WHERE (
                    COALESCE(v.salary_from, 0) > (SELECT avg_value FROM avg_salary)
                    OR
                    COALESCE(v.salary_to, 0) > (SELECT avg_value FROM avg_salary)
                )
                AND (v.salary_from IS NOT NULL OR v.salary_to IS NOT NULL);
            """
            cur.execute(sql_query)
            results = cur.fetchall()

        except Exception as e:
            print(f"❌ Ошибка при выполнении запроса 'get_vacancies_with_higher_salary': {e}")
            results = []

        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

        return results


def save_employers_to_db(employers_data, params):
    """
    Сохраняет данные о работодателях в таблицу 'employers'.
    Использует ON CONFLICT DO NOTHING.
    """
    conn = None
    cur = None

    try:
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        insert_employer_sql = """
            INSERT INTO employers (employer_id, employer_name, employer_url)
            VALUES (%s, %s, %s)
            ON CONFLICT (employer_id) DO NOTHING;
        """
        for employer in employers_data:
            employer_id = employer.get('id')
            employer_name = employer.get('name')
            employer_url = employer.get('alternate_url')
            employer_values = (employer_id, employer_name, employer_url)
            cur.execute(insert_employer_sql, employer_values)

        conn.commit()

    except psycopg2.errors.UniqueViolation as e:
         print(f"ℹ️ Попытка вставить дубликат работодателя: {e}")
         if conn:
             conn.rollback()
    except Exception as e:
        print(f"❌ Ошибка при загрузке данных работодателей: {e}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def save_vacancies_to_db(companies_data, params):
    """
    Сохраняет данные о вакансиях из списка компаний в таблицу 'vacancies'.
    Использует ON CONFLICT DO NOTHING.

    Args:
        companies_data (list): Список словарей с данными о компаниях (как из API),
                               каждый словарь содержит список вакансий.
        params (dict): Параметры подключения к PostgreSQL.
    """
    conn = None
    cur = None

    try:
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        insert_vacancy_sql = """
            INSERT INTO vacancies (vacancy_id, employer_id, title, salary_from, salary_to, salary_currency, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (vacancy_id) DO NOTHING;
        """

        # Перебираем каждую компанию в списке
        for company_data in companies_data:
            employer_id = company_data.get('id') # Получаем ID работодателя для вакансий этой компании

            # Проверяем, есть ли у компании список вакансий и что он не пустой
            vacancies_list = company_data.get('vacancies')
            if not vacancies_list:
                continue # Если вакансий нет, переходим к следующей компании

            # Перебираем каждую вакансию в списке вакансий этой компании
            for vacancy in vacancies_list:
                vacancy_id = vacancy.get('id')
                title = vacancy.get('name') # Название вакансии в данных API - 'name'
                url = vacancy.get('alternate_url') # Ссылка на вакансию

                # Извлекаем данные о зарплате, если они есть
                salary_data = vacancy.get('salary')
                salary_from = None
                salary_to = None
                salary_currency = None

                if salary_data: # Проверяем, есть ли поле 'salary'
                    salary_from = salary_data.get('from')
                    salary_to = salary_data.get('to')
                    salary_currency = salary_data.get('currency')

                # Собираем данные вакансии в кортеж.
                # Порядок ДОЛЖЕН совпадать с порядком столбцов в INSERT!
                vacancy_values = (
                    vacancy_id,
                    employer_id, # Используем ID работодателя, полученный из внешнего цикла
                    title,
                    salary_from,
                    salary_to,
                    salary_currency,
                    url
                )

                # Выполняем SQL команду для вставки данных одной вакансии
                cur.execute(insert_vacancy_sql, vacancy_values)

        conn.commit()

    except psycopg2.errors.UniqueViolation as e:
         print(f"ℹ️ Попытка вставить дубликат вакансии: {e}")
         if conn:
             conn.rollback()
    except Exception as e:
        print(f"❌ Ошибка при загрузке данных вакансий: {e}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

