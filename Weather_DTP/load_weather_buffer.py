"""
v2_load_weather_buffer.py
-------------------------
Версия 2. Загружает архивные данные о погоде (Open-Meteo Archive API)
и сохраняет результаты в таблицу weather_history.


Основные точки входа:
    load_weather_for_cities(city_names, start_year, end_year)
        — полная загрузка за диапазон лет (используется в v2_main.py)
    load_weather_for_last_month(year, month)
        — загрузка только за один месяц (используется в updater.py)

Зависимости:
    pip install supabase python-dotenv requests pandas matplotlib

Переменные окружения (.env):
    SUPABASE_URL=https://xxxx.supabase.co
    SUPABASE_KEY=eyJ...   # service_role ключ
"""

import os
import requests
import pandas as pd
import matplotlib.pyplot as plt
from supabase import create_client, Client
from dotenv import load_dotenv
import time
import logging
from datetime import datetime, timezone, date
import calendar

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

log = logging.getLogger(__name__)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ─────────────────────────── Вспомогательные функции ──────────────────────────

def get_valid_daily_params():
    """Параметры, реально поддерживаемые Archive API."""
    return [
        "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
        "apparent_temperature_max", "apparent_temperature_min", "apparent_temperature_mean",
        "precipitation_sum", "rain_sum", "snowfall_sum", "precipitation_hours",
        "weather_code", "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant",
        "shortwave_radiation_sum", "et0_fao_evapotranspiration",
        "sunrise", "sunset", "daylight_duration", "sunshine_duration"
    ]


def get_season(month):
    if month in [12, 1, 2]: return "winter"
    if month in [3, 4, 5]: return "spring"
    if month in [6, 7, 8]: return "summer"
    return "autumn"


# ─────────────────────────── Open-Meteo геокодирование ────────────────────────

def resolve_city_via_openmeteo(city_name_ru: str) -> dict | None:
    """
    Уточняет координаты города через Open-Meteo Geocoding API.
    Возвращает {name_en, lat, lon} или None если не найдено.
    """
    try:
        resp = requests.get(
            "https://geocoding-api.open-meteo.com/v1/search",
            params={
                "name":         city_name_ru,
                "count":        5,
                "language":     "ru",
                "format":       "json",
                "country_code": "RU",
            },
            timeout=10
        )
        data    = resp.json()
        results = data.get("results", [])

        if not results:
            log.warning(f"  Open-Meteo не нашёл город: {city_name_ru}")
            return None

        best    = results[0]
        name_en = best.get("name", city_name_ru)
        lat     = best.get("latitude")
        lon     = best.get("longitude")

        log.info(f"  [{city_name_ru}] → {name_en} (lat={lat:.4f}, lon={lon:.4f})")
        return {"name_en": name_en, "lat": lat, "lon": lon}

    except Exception as e:
        log.error(f"  Ошибка геокодирования Open-Meteo для {city_name_ru}: {e}")
        return None


# ─────────────────────────── Чтение городов из БД ─────────────────────────────

def fetch_cities_from_db(city_names: list[str] = None) -> list[dict]:
    """
    Загружает города из таблицы cities (рабочий набор, v2).
    Если city_names задан — фильтрует только по ним.
    """
    query = supabase.table("cities").select("city_name, lat, lon")

    if city_names:
        query = query.in_("city_name", city_names)

    response = query.execute()

    if not response.data:
        log.warning("Таблица cities пуста или города не найдены")
        return []

    cities = [
        c for c in response.data
        if c.get("lat") is not None and c.get("lon") is not None
    ]

    skipped = len(response.data) - len(cities)
    if skipped > 0:
        log.warning(f"Пропущено {skipped} городов без координат")

    log.info(f"Загружено городов для обработки: {len(cities)}")
    return cities


# ─────────────────────────── Обновление даты в cities ─────────────────────────

def update_last_weather_update(city_name: str):
    """
    Обновляет поле last_weather_update в таблице cities.
    Вызывается после успешной загрузки погоды для города.
    """
    try:
        now = datetime.now(timezone.utc).isoformat()
        supabase.table("cities").update(
            {"last_weather_update": now}
        ).eq("city_name", city_name).execute()
        log.info(f"  [{city_name}] last_weather_update обновлён: {now}")
    except Exception as e:
        log.warning(f"  [{city_name}] Не удалось обновить last_weather_update: {e}")


# ─────────────────────────── Сохранение в Supabase ────────────────────────────

def save_to_supabase(df, city_name):
    """Сохраняет DataFrame в Supabase с корректной сериализацией."""
    try:
        df_to_save = df.copy()

        if 'time' in df_to_save.columns:
            df_to_save['time'] = df_to_save['time'].dt.strftime('%Y-%m-%d')

        for col in ['sunrise', 'sunset']:
            if col in df_to_save.columns:
                if pd.api.types.is_datetime64_any_dtype(df_to_save[col]):
                    df_to_save[col] = df_to_save[col].apply(
                        lambda x: x.strftime('%Y-%m-%dT%H:%M:%S') if pd.notna(x) else None
                    )
                else:
                    df_to_save[col] = df_to_save[col].where(pd.notna(df_to_save[col]), None)

        df_to_save = df_to_save.where(pd.notna(df_to_save), None)
        records    = df_to_save.to_dict(orient='records')

        supabase.table("weather_history").upsert(
            records, on_conflict="city_name,time"
        ).execute()

        log.info(f"Данные за {df['year'].iloc[0]} сохранены в Supabase ({len(df)} дней)")

    except Exception as e:
        log.error(f"Ошибка при сохранении в Supabase: {e}")
        if 'records' in locals() and records:
            log.debug("Первая запись (для отладки):")
            for k, v in records[0].items():
                log.debug(f"   {k}: {type(v).__name__} = {v}")


# ─────────────────────────── Загрузка погоды ──────────────────────────────────

def get_weather_for_period(
    lat:        float,
    lon:        float,
    city_name:  str,
    start_date: str,
    end_date:   str,
) -> pd.DataFrame | None:
    """
    Загружает данные о погоде за произвольный период (start_date–end_date).
    Разбивает период на годовые отрезки для стабильности API.

    :param start_date: начало периода в формате YYYY-MM-DD
    :param end_date:   конец периода в формате YYYY-MM-DD
    :return: итоговый DataFrame или None
    """
    start = date.fromisoformat(start_date)
    end   = date.fromisoformat(end_date)
    all_data = []

    # Разбиваем на годовые отрезки
    current_year = start.year
    while current_year <= end.year:
        year_start = date(current_year, 1,  1) if current_year != start.year else start
        year_end   = date(current_year, 12, 31) if current_year != end.year   else end

        log.info(f"  [{city_name}] загружаю {year_start} – {year_end}...")

        params = {
            "latitude":   lat,
            "longitude":  lon,
            "start_date": str(year_start),
            "end_date":   str(year_end),
            "daily":      get_valid_daily_params(),
            "timezone":   "auto",
        }

        try:
            resp = requests.get(
                "https://archive-api.open-meteo.com/v1/archive",
                params=params, timeout=30
            )
            if resp.status_code != 200:
                log.warning(f"  [{city_name}] HTTP {resp.status_code} за {current_year}")
                current_year += 1
                continue

            data = resp.json()
            if "daily" not in data or not data["daily"]["time"]:
                log.warning(f"  [{city_name}] нет данных за {current_year}")
                current_year += 1
                continue

            df             = pd.DataFrame(data["daily"])
            df["time"]     = pd.to_datetime(df["time"])

            for col in ['sunrise', 'sunset']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])

            df["city_name"] = city_name
            df["latitude"]  = lat
            df["longitude"] = lon
            df["year"]      = df["time"].dt.year
            df["month"]     = df["time"].dt.month
            df["season"]    = df["month"].apply(get_season)

            all_data.append(df)
            save_to_supabase(df, city_name)

        except Exception as e:
            log.error(f"  [{city_name}] исключение за {current_year}: {e}")

        time.sleep(1.5)
        current_year += 1

    if not all_data:
        log.warning(f"[{city_name}] данные не получены ни за один период")
        return None

    return pd.concat(all_data, ignore_index=True)


# ─────────────────────────── Основные точки входа ─────────────────────────────

def load_weather_for_cities(
    city_names: list[str] = None,
    start_year: int = 2014,
    end_year:   int = 2024,
):
    """
    Полная загрузка погоды за диапазон лет.
    Используется в v2_main.py при первоначальной загрузке.

    После загрузки каждого города обновляет last_weather_update в cities.

    :param city_names: список городов или None (все из таблицы cities)
    :param start_year: начальный год
    :param end_year:   конечный год
    """
    cities = fetch_cities_from_db(city_names)

    if not cities:
        log.error("Нет городов для обработки — завершение")
        return

    log.info(f"Загрузка погоды за {start_year}–{end_year} для {len(cities)} городов")

    for i, city in enumerate(cities, 1):
        name_ru = city["city_name"]
        log.info(f"[{i}/{len(cities)}] {name_ru}")

        resolved = resolve_city_via_openmeteo(name_ru)
        if resolved is None:
            log.warning(f"  Пропускаем {name_ru} — не удалось получить координаты")
            continue

        lat = resolved["lat"]
        lon = resolved["lon"]

        result = get_weather_for_period(
            lat=lat, lon=lon, city_name=name_ru,
            start_date=f"{start_year}-01-01",
            end_date=f"{end_year}-12-31",
        )

        if result is not None:
            update_last_weather_update(name_ru)

        time.sleep(0.5)

    log.info("Загрузка погоды завершена")


def load_weather_for_last_month(year: int, month: int):
    """
    Загрузка погоды только за один конкретный месяц для всех городов из cities.
    Используется в updater.py при ежемесячном обновлении.

    После успешной загрузки обновляет last_weather_update в таблице cities.

    :param year:  год
    :param month: месяц (1–12)
    """
    cities = fetch_cities_from_db()

    if not cities:
        log.error("Нет городов для обработки — завершение")
        return

    # Первый и последний день указанного месяца
    last_day    = calendar.monthrange(year, month)[1]
    start_date  = f"{year}-{month:02d}-01"
    end_date    = f"{year}-{month:02d}-{last_day:02d}"

    log.info(f"Загрузка погоды за {start_date} – {end_date} для {len(cities)} городов")

    for i, city in enumerate(cities, 1):
        name_ru = city["city_name"]
        log.info(f"[{i}/{len(cities)}] {name_ru}")

        resolved = resolve_city_via_openmeteo(name_ru)
        if resolved is None:
            log.warning(f"  Пропускаем {name_ru} — не удалось получить координаты")
            continue

        result = get_weather_for_period(
            lat=resolved["lat"], lon=resolved["lon"], city_name=name_ru,
            start_date=start_date,
            end_date=end_date,
        )

        if result is not None:
            update_last_weather_update(name_ru)

        time.sleep(0.5)

    log.info("Загрузка погоды за месяц завершена")


def visualize_data(df, city_name):
    if df is None or df.empty:
        return

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle(f'{city_name}', fontsize=16)

    if 'temperature_2m_mean' in df.columns:
        axes[0,0].plot(df["time"], df["temperature_2m_mean"], lw=0.5, color='red', alpha=0.7)
        axes[0,0].set_title("Средняя температура")
        axes[0,0].set_ylabel("°C")
        axes[0,0].grid(True, alpha=0.3)

    if 'precipitation_sum' in df.columns:
        axes[0,1].bar(df["time"], df["precipitation_sum"], width=1, color='blue', alpha=0.6)
        axes[0,1].set_title("Осадки")
        axes[0,1].set_ylabel("мм")
        axes[0,1].grid(True, alpha=0.3)

    if 'wind_speed_10m_max' in df.columns:
        axes[1,0].plot(df["time"], df["wind_speed_10m_max"], lw=0.5, color='green', alpha=0.7)
        axes[1,0].set_title("Макс. ветер")
        axes[1,0].set_ylabel("км/ч")
        axes[1,0].grid(True, alpha=0.3)

    if 'temperature_2m_mean' in df.columns and 'month' in df.columns:
        monthly = [df[df['month']==m]['temperature_2m_mean'].dropna() for m in range(1,13)]
        axes[1,1].boxplot(monthly)
        axes[1,1].set_title("Температура по месяцам")
        axes[1,1].set_xlabel("Месяц")
        axes[1,1].set_ylabel("°C")
        axes[1,1].set_xticklabels(['Я','Ф','М','А','М','И','И','А','С','О','Н','Д'])
        axes[1,1].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(f'{city_name}_analysis.png', dpi=150)
    plt.show()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    load_weather_for_cities(
        city_names=["Москва", "Санкт-Петербург"],
        start_year=2014,
        end_year=2024,
    )