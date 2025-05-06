# main.py - Главный скрипт для запуска процесса сбора данных и работы с БД

from db import save_employers_to_db, save_vacancies_to_db, DBManager, db_params, create_database, create_tables
from api_HH import get_company_data
import json


# --- ШАГ 0: Создание базы данных и таблиц ---
print("\n--- Создание базы данных и таблиц ---")
create_database(db_params)
create_tables(db_params)
print("--- Создание базы данных и таблиц завершено ---\n")

# --- ШАГ 1: Сбор данных с HH.ru ---
print("\n--- Запуск процесса сбора данных с HH.ru ---")

all_employers_ids = [
    6086392, 9498112, 3529, 1565051, 1947314,
    78638, 7944, 2374897, 6093775, 906391
]

all_companies_data = []

for employer_id in all_employers_ids:
    print(f'\n Получаем данные для компании с ID: {employer_id}')
    company_data = get_company_data(employer_id)
    all_companies_data.append(company_data)

print("\n--- Процесс сбора данных завершен ---")
# --- Конец ШАГ 1 ---


# --- ШАГ 2: Загрузка данных в БД ---
print("\n--- Загрузка данных в БД ---")

print("--- Загрузка данных о работодателях в БД ---")
save_employers_to_db(all_companies_data, db_params)
print("--- Загрузка данных о работодателях завершена ---")

print("\n--- Загрузка данных о вакансиях в БД ---")
save_vacancies_to_db(all_companies_data, db_params)
print("--- Загрузка данных о вакансиях завершена ---")
# --- Конец ШАГ 2 ---


# --- ШАГ 3: Работа с базой данных через DBManager ---
print("\n--- Работа с базой данных через DBManager ---")

db_manager = DBManager(db_params)

# --- Выполнение запросов через DBManager ---
print("\n--- Выполнение запросов через DBManager ---")

# 1. Получаем список всех компаний и количество открытых вакансий у каждой
print("\nЗапрос 1: Количество вакансий у каждой компании:")
companies_with_vacancy_count = db_manager.get_companies_and_vacancies_count()
if companies_with_vacancy_count:
    for row in companies_with_vacancy_count:
        print(f"Компания: {row[0]}, Вакансий: {row[1]}")
else:
    print("Нет данных или произошла ошибка при выполнении запроса 1.")
print("------------------------------------------")

# 2. Получаем список всех вакансий
print("\nЗапрос 2: Список всех вакансий:")
all_vacancies = db_manager.get_all_vacancies()
if all_vacancies:
    print(f"Найдено всего вакансий: {len(all_vacancies)}. Первые 5:")
    for i, row in enumerate(all_vacancies[:5]):
         print(f"  ID: {row[0]}, Компания: {row[1]}, Заголовок: {row[2]}, Зарплата: {row[3]}-{row[4]} {row[5]}, URL: {row[6]}")
    if len(all_vacancies) > 5:
        print("  ...")
else:
    print("Нет данных или произошла ошибка при выполнении запроса 2.")
print("------------------------------------------")


# 3. Получаем список всех вакансий с указанной зарплатой
print("\nЗапрос 3: Список вакансий с зарплатой:")
vacancies_with_salary = db_manager.get_vacancies_with_salary()
if vacancies_with_salary:
    print(f"Найдено вакансий с зарплатой: {len(vacancies_with_salary)}. Первые 5:")
    for i, row in enumerate(vacancies_with_salary[:5]):
         print(f"  ID: {row[0]}, Заголовок: {row[2]}, Зарплата: {row[3]}-{row[4]} {row[5]}, URL: {row[6]}")
    if len(vacancies_with_salary) > 5:
        print("  ...")
else:
    print("Нет данных или произошла ошибка при выполнении запроса 3.")
print("------------------------------------------")


# 4. Получаем список всех вакансий, в названии которых содержится ключевое слово
keyword_to_search = input("Введите ключевое слово для поиска в вакансиях (например, Python): ")
print(f"\nЗапрос 4: Список вакансий с ключевым словом '{keyword_to_search}':")
vacancies_with_keyword = db_manager.get_vacancies_with_keyword(keyword_to_search)
if vacancies_with_keyword:
    print(f"Найдено вакансий с ключевым словом: {len(vacancies_with_keyword)}. Первые 5:")
    for i, row in enumerate(vacancies_with_keyword[:5]):
         print(f"  ID: {row[0]}, Заголовок: {row[2]}, Зарплата: {row[3]}-{row[4]} {row[5]}, URL: {row[6]}")
    if len(vacancies_with_keyword) > 5:
        print("  ...")
else:
    print("Нет данных или произошла ошибка при выполнении запроса 4.")
print("------------------------------------------")

# 5. Получаем среднюю зарплату по всем вакансиям
print("\nЗапрос 5: Средняя зарплата по всем вакансиям:")
avg_salary = db_manager.get_avg_salary()
if avg_salary is not None:
    print(f"Средняя зарплата: {avg_salary:.2f}")
else:
    print("Нет данных о зарплатах или произошла ошибка при выполнении запроса 5.")
print("------------------------------------------")

# 6. Получаем список вакансий с зарплатой выше средней
print("\nЗапрос 6: Список вакансий с зарплатой выше средней:")
vacancies_higher_salary = db_manager.get_vacancies_with_higher_salary()
if vacancies_higher_salary:
    print(f"Найдено вакансий с зарплатой выше средней: {len(vacancies_higher_salary)}. Первые 5:")
    for i, row in enumerate(vacancies_higher_salary[:5]):
        # Последний элемент в кортеже - название компании из JOIN
        employer_name = row[-1]
        print(f"  ID: {row[0]}, Компания: {employer_name}, Заголовок: {row[2]}, Зарплата: {row[3]}-{row[4]} {row[5]}, URL: {row[6]}")
    if len(vacancies_higher_salary) > 5:
        print("  ...")
else:
    print("Нет данных или произошла ошибка при выполнении запроса 6.")
print("------------------------------------------")

print("\n--- Работа с базой данных через DBManager завершена ---")
# --- Конец ШАГ 3 ---


# --- ШАГ 4: Вывод всех собранных данных (для справки, можно удалить) ---
print("\n--- Вывод всех собранных данных о компаниях (из main.py) ---")
print(json.dumps(all_companies_data, indent=4, ensure_ascii=False))
print("------------------------------------")
# --- Конец ШАГ 4 ---
