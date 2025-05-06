import requests
import json


HH_API = 'https://api.hh.ru/' #"Пожалуйста, отправь HTTP GET-запрос (запрос на получение данных) на вот этот адрес: employer_url"


def get_company_data(employer_id):
    """Получает данные о работодателе по ID.
    
    Args:
        employer_id (int): Идентификатор работодателя на HH.ru
        
    Returns:
        dict: Словарь с данными о работодателе
    """

    employer_url = f'{HH_API}employers/{employer_id}'


    #Выводим в консоль URL, который сейчас будем запрашивать.
    #Почему: Это **очень полезно** для **отладки**! Если что-то пойдет не так, ты сразу увидишь, куда именно отправлялся запрос. 👀

    print(f'Запрашиваем данные по URL: {employer_url}')

    #Отправляем ** основной ** запрос к API hh.ru, используя библиотеку `requests`.
    # Почему: Метод `.get()` отправляет запрос на получение данных с сервера по указанному адресу. 🌐
    response = requests.get(employer_url) #"Пожалуйста, отправь HTTP GET-запрос (запрос на получение данных) на вот этот адрес: employer_url"
    response.raise_for_status()
    # 4. Преобразуем ответ из JSON в словарь Python
    employer_data = response.json()
    print(f'Данные по работодателю {employer_id} успешно получены')
    
    # Получаем вакансии для этого работодателя
    employer_data['vacancies'] = get_company_vacancies(employer_id)
    
    return employer_data


def get_company_vacancies(employer_id):
    """Получает список вакансий компании по ID работодателя.
    
    Args:
        employer_id (int): Идентификатор работодателя на HH.ru
        
    Returns:
        list: Список словарей с данными о вакансиях
    """
    
    # Формируем URL для запроса вакансий с параметром поиска по работодателю
    vacancies_url = f'{HH_API}vacancies'
    
    # Параметры запроса: ищем вакансии только указанного работодателя
    # per_page=100 - максимальное количество вакансий на странице
    params = {
        'employer_id': employer_id,
        'per_page': 100
    }
    
    print(f'Запрашиваем вакансии работодателя {employer_id}...')
    
    try:
        response = requests.get(vacancies_url, params=params)
        response.raise_for_status()
        
        # Получаем данные о вакансиях из ответа
        vacancies_data = response.json()
        vacancies_list = vacancies_data.get('items', [])
        
        print(f'Получено {len(vacancies_list)} вакансий для работодателя {employer_id}')
        return vacancies_list
        
    except Exception as e:
        print(f'❌ Ошибка при получении вакансий работодателя {employer_id}: {e}')
        return []  # Возвращаем пустой список в случае ошибки
