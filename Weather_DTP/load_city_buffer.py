import os
import requests
import pandas as pd
import warnings
from supabase import create_client, Client
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
YANDEX_APIKEY = os.getenv("YANDEX_APIKEY")  # ключ геокодера Яндекса

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

warnings.filterwarnings('ignore')


def fetch_coordinates(apikey, address):
    """
    Получение координат по адресу через геокодер Яндекса.

    :param apikey: API ключ для доступа к геокодеру яндекса
    :type apikey: str
    :param address: адрес или название города
    :type address: str
    :return: (lon, lat) или (None, None) если не найдено
    """
    base_url = "https://geocode-maps.yandex.ru/1.x"
    response = requests.get(base_url, params={
        "geocode": address,
        "apikey": apikey,
        "format": "json",
    })
    response.raise_for_status()  # Проверка на ошибки HTTP
    found_places = response.json()['response']['GeoObjectCollection']['featureMember']
    if not found_places:
        return None, None
    # Берём первый и самый релевантный результат
    most_relevant = found_places[0]
    # Координаты в ответе: "долгота широта"
    lon, lat = most_relevant['GeoObject']['Point']['pos'].split(" ")
    return lon, lat


def fetch_cities_from_wikipedia():
    """
    Загружает список городов России из Википедии через BeautifulSoup
    и возвращает DataFrame с городами с населением > 100 000.
    """
    from bs4 import BeautifulSoup
    import re

    # Определяем параметры запроса
    params = {
        'action': 'parse',
        'page': 'Список_городов_России',
        'format': 'json',
        'prop': 'text',
        'contentmodel': 'wikitext'
    }
    # Указываем заголовок — Вики ругается, если его нет
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    # Отправляем запрос
    response = requests.get(
        'https://ru.wikipedia.org/w/api.php',
        params=params, headers=headers
    )

    # Проверяем успешность запроса
    if response.status_code != 200:
        print(f"❌ Ошибка {response.status_code}: {response.text}")
        return None

    print("✅ Запрос к Википедии успешен!")
    data = response.json()  # Преобразуем JSON в словарь Python

    # Извлекаем HTML (в JSON WIKI данные лежат по пути parse→text)
    html_content = data['parse']['text']['*']

    # Парсим HTML через BeautifulSoup и ищем wikitable
    soup = BeautifulSoup(html_content, 'html.parser')
    # Ищем главную таблицу городов (класс 'standard sortable')
    table = soup.find('table', {'class': 'standard'})

    if table is None:
        print("❌ Таблица не найдена в HTML")
        return None

    print("✅ Таблица найдена, парсим строки...")

    rows = []
    for tr in table.find_all('tr')[1:]:  # пропускаем заголовок
        cols = tr.find_all(['td', 'th'])
        if len(cols) < 6:
            continue
        # Извлекаем текст, убирая сноски и лишние пробелы
        def cell_text(cell):
            return cell.get_text(separator=' ', strip=True)

        rows.append({
            'city_name':  cell_text(cols[2]),   # название города
            'region':     cell_text(cols[3]),   # субъект РФ
            'federal':    cell_text(cols[4]),   # федеральный округ
            'population': cell_text(cols[5]),   # население
        })

    if not rows:
        print("❌ Строки из таблицы не извлечены")
        return None

    df = pd.DataFrame(rows)

    # Сохраняем в CSV для анализа
    df.to_csv('cities_russia.csv', index=False, encoding='utf-8')
    print(f"💾 Сохранено {len(df)} городов в cities_russia.csv")

    # Чистим данные
    df['city_name'] = df['city_name'].str.replace('не призн.', '').str.strip()
    df['population'] = (
        df['population']
        .astype(str)
        .str.replace('\xa0', '')   # неразрывный пробел
        .str.replace(' ', '')
        .str.split('[').str[0]       # убираем сноски вида [1]
        .apply(lambda x: re.sub(r'\D', '', x))  # оставляем только цифры
    )
    df['population'] = pd.to_numeric(df['population'], errors='coerce')
    df = df.dropna(subset=['population'])
    df['population'] = df['population'].astype(int)

    # Оставляем только города с населением > 100 000
    to_work = df[df['population'] > 100000].reset_index(drop=True)
    print(f"🏙️  Городов с населением > 100 000: {len(to_work)}")

    return to_work


def enrich_with_coordinates(df, apikey):
    """
    Добавляет координаты (lat, lon) к каждому городу через Яндекс.Геокодер.
    """
    df['lat'] = None
    df['lon'] = None

    for idx, row in df.iterrows():
        city_name = str(row['city_name'])
        try:
            lon, lat = fetch_coordinates(apikey, city_name)
            df.at[idx, 'lon'] = lon
            df.at[idx, 'lat'] = lat
        except Exception as e:
            print(f"  ⚠️ Ошибка для {city_name}: {e}")

    return df


def upload_to_supabase(df):
    """
    Загружает DataFrame с городами в таблицу cities в Supabase.
    При повторном запуске обновляет существующие записи (upsert по city_name).
    """
    records = df.copy()

    # Приводим числовые типы к стандартным Python-типам для сериализации
    records['population'] = records['population'].apply(
        lambda x: int(x) if pd.notna(x) else None
    )
    records['lat'] = records['lat'].apply(
        lambda x: float(x) if pd.notna(x) else None
    )
    records['lon'] = records['lon'].apply(
        lambda x: float(x) if pd.notna(x) else None
    )

    # Заменяем NaN на None (NULL в БД)
    records = records.where(pd.notna(records), None)
    rows = records.to_dict(orient='records')

    # Вставляем чанками по 100 записей
    chunk_size = 100
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        try:
            supabase.table("cities_buffer").upsert(chunk, on_conflict="city_name").execute()
            print(f"✅ Вставлен чанк {i // chunk_size + 1} ({len(chunk)} записей)")
        except Exception as e:
            print(f"❌ Ошибка вставки чанка {i // chunk_size + 1}: {e}")

    print(f"\n✅ Загрузка завершена. Всего записей: {len(rows)}")


if __name__ == "__main__":
    # 1. Загружаем список городов из Википедии
    cities_df = fetch_cities_from_wikipedia()
    if cities_df is None:
        print("❌ Не удалось получить данные из Википедии")
        exit(1)

    # 2. Обогащаем координатами через Яндекс.Геокодер
    cities_df = enrich_with_coordinates(cities_df, YANDEX_APIKEY)
    print(cities_df)

    # 3. Загружаем в Supabase
    upload_to_supabase(cities_df)