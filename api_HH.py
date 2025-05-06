import requests
import json


HH_API = 'https://api.hh.ru/' #"Пожалуйста, отправь HTTP GET-запрос (запрос на получение данных) на вот этот адрес: employer_url"


def get_company_data(employer_id):
    """Получает данные о работодателе и его вакансиях по ID."""

    employer_url = f'{HH_API}employers/{employer_id}'


#Выводим в консоль URL, который сейчас будем запрашивать.
#Почему: Это **очень полезно** для **отладки**! Если что-то пойдет не так, ты сразу увидишь, куда именно отправлялся запрос. 👀

    print(f'Запрашиваем данные  по URL: {employer_url}')

#Отправляем ** основной ** запрос к API hh.ru, используя библиотеку `requests`.
    # Почему: Метод `.get()` отправляет запрос на получение данных с сервера по указанному адресу. 🌐
    response = requests.get(employer_url) #"Пожалуйста, отправь HTTP GET-запрос (запрос на получение данных) на вот этот адрес: employer_url"
    response.raise_for_status()
    # 4. Преобразуем ответ из JSON в словарь Python
    employer_data = response.json()
    print(f'данные по работадателю {employer_id} успешно получены')
    return employer_data
