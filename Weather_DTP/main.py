"""
v2_main.py
----------
Версия 2. Скрипт первоначальной загрузки данных.
Запускается однократно для инициализации проекта.

Пайплайн:
    Шаг 0 — создание таблиц (IF NOT EXISTS)
    Шаг 1 — загрузка городов из Википедии
    Шаг 2 — геокодирование через Яндекс
    Шаг 3 — сохранение в cities_buffer
    Шаг 4 — выбор городов пользователем + копирование в таблицу cities
    Шаг 5 — загрузка ДТП ((текущий год − 10)–текущий год)
    Шаг 6 — трансформация dtp_buffer → нормализованные таблицы
    Шаг 7 — загрузка погоды ((текущий год − 10)–текущий год)
"""

import os
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client

from load_city_buffer import fetch_cities_from_wikipedia, enrich_with_coordinates
from load_city_buffer import upload_to_supabase as upload_cities_to_buffer
from load_weather_buffer import load_weather_for_cities
from load_dtp_buffer import load_dtp_for_cities, find_districts_by_city_names
from transform_dtp_buffer import transform_and_load as transform_dtp
from create_target_tables import create_tables

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
YANDEX_APIKEY = os.getenv("YANDEX_APIKEY")

# ─────────────────────── Временной диапазон загрузки ──────────────────────────
# Диапазон вычисляется автоматически на основе даты запуска скрипта:
#   END_YEAR   = текущий год (например, 2026)
#   START_YEAR = текущий год − 10 (например, 2016)
# Таким образом всегда загружается полное 10-летнее окно,
# включая полный стартовый год и текущий год до последнего доступного месяца.

_NOW               = datetime.now()
END_YEAR           = _NOW.year           # 2026 при запуске в 2026
START_YEAR         = END_YEAR - 10       # 2016 при запуске в 2026

DTP_START_YEAR     = START_YEAR
DTP_END_YEAR       = END_YEAR
WEATHER_START_YEAR = START_YEAR
WEATHER_END_YEAR   = END_YEAR

# ─────────────────────────── Логирование ──────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("main_v2.log", encoding="utf-8"),
    ]
)
log = logging.getLogger(__name__)


# ─────────────────────────── Работа с таблицей cities ─────────────────────────

def copy_cities_to_working_table(city_names: list[str]):
    """
    Копирует выбранные города из cities_buffer в таблицу cities.
    Upsert по city_name — повторный запуск безопасен.
    Поля last_dtp_update и last_weather_update при первом добавлении = NULL.
    """
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    try:
        resp = supabase.table("cities_buffer").select(
            "city_name, region, federal, population, lat, lon"
        ).in_("city_name", city_names).execute()
    except Exception as e:
        log.error(f"Ошибка при чтении cities_buffer: {e}")
        return

    if not resp.data:
        log.error("Не найдено ни одного города в cities_buffer")
        return

    rows = [
        {
            "city_name":  r["city_name"],
            "region":     r["region"],
            "federal":    r["federal"],
            "population": r["population"],
            "lat":        r["lat"],
            "lon":        r["lon"],
            # last_dtp_update и last_weather_update — NULL при первом добавлении
        }
        for r in resp.data
    ]

    try:
        supabase.table("cities").upsert(rows, on_conflict="city_name").execute()
        log.info(f"Скопировано в таблицу cities: {len(rows)} городов")
    except Exception as e:
        log.error(f"Ошибка при копировании в cities: {e}")


# ─────────────────────────── Выбор городов пользователем ──────────────────────

def select_cities() -> list[str] | None:
    """
    Интерактивный ввод городов пользователем.
    Проверяет каждый город по таблице cities_buffer.
    Возвращает список городов или None (все города).
    """
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    try:
        resp      = supabase.table("cities_buffer").select("city_name").execute()
        available = {row["city_name"] for row in resp.data}
    except Exception as e:
        log.error(f"Не удалось загрузить список городов из БД: {e}")
        return None

    print("\n" + "=" * 60)
    print("Введите названия городов для загрузки данных.")
    print("Несколько городов — через запятую. Например: Москва, Казань")
    print("Для загрузки всех городов оставьте поле пустым и нажмите Enter.")
    print("=" * 60)

    while True:
        raw = input("\nГорода: ").strip()

        if not raw:
            log.info("Выбраны все города из таблицы")
            return None

        entered   = [c.strip() for c in raw.split(",") if c.strip()]
        not_found = [c for c in entered if c not in available]

        if not_found:
            print(f"\n❌ Города не найдены в базе данных: {', '.join(not_found)}")
            print(f"   Доступно городов в БД: {len(available)}")
            print("   Проверьте написание и попробуйте ещё раз.\n")
            continue

        print(f"\n✅ Выбрано городов: {len(entered)}: {', '.join(entered)}")
        return entered


# ─────────────────────────── Основная функция ─────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("v2 — Запуск пайплайна первоначальной загрузки данных")
    log.info("=" * 60)

    # ── 0. Создание таблиц ─────────────────────────────────────────────────────
    log.info("Шаг 0: Создание таблиц в базе данных (если не существуют)")
    try:
        create_tables()
        log.info("Таблицы готовы")
    except Exception as e:
        log.exception(f"Ошибка при создании таблиц: {e}")
        return

    # ── 1. Загрузка городов из Википедии ──────────────────────────────────────
    log.info("Шаг 1: Загрузка списка городов из Википедии")
    try:
        cities_df = fetch_cities_from_wikipedia()
        if cities_df is None:
            log.error("Не удалось получить данные о городах — завершение")
            return
        log.info(f"Получено городов: {len(cities_df)}")
    except Exception as e:
        log.exception(f"Ошибка при загрузке городов: {e}")
        return

    # ── 2. Геокодирование ─────────────────────────────────────────────────────
    log.info("Шаг 2: Геокодирование городов через Яндекс")
    try:
        cities_df = enrich_with_coordinates(cities_df, YANDEX_APIKEY)
        log.info("Геокодирование завершено")
    except Exception as e:
        log.exception(f"Ошибка при геокодировании: {e}")
        return

    # ── 3. Сохранение в cities_buffer ─────────────────────────────────────────
    log.info("Шаг 3: Загрузка городов в cities_buffer")
    try:
        upload_cities_to_buffer(cities_df)
        log.info("Города успешно загружены в cities_buffer")
    except Exception as e:
        log.exception(f"Ошибка при загрузке в cities_buffer: {e}")
        return

    # ── 4. Выбор городов + копирование в cities ───────────────────────────────
    log.info("Шаг 4: Выбор городов и формирование рабочего набора")
    selected_cities = select_cities()
    if selected_cities is None:
        log.info("Выбраны все города из таблицы")
    else:
        log.info(f"Выбранные города: {selected_cities}")

    # Определяем список для копирования
    if selected_cities:
        cities_to_copy = selected_cities
    else:
        # Берём все из cities_buffer
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        resp = supabase.table("cities_buffer").select("city_name").execute()
        cities_to_copy = [r["city_name"] for r in resp.data]

    copy_cities_to_working_table(cities_to_copy)
    log.info(f"Рабочий набор cities сформирован: {len(cities_to_copy)} городов")

    # ── 5. Загрузка ДТП ───────────────────────────────────────────────────────
    log.info(f"Шаг 5: Загрузка ДТП ({DTP_START_YEAR}–{DTP_END_YEAR})")
    try:
        load_dtp_for_cities(
            city_names=selected_cities if selected_cities else cities_to_copy,
            json_path="regions_all.json",
            start_year=DTP_START_YEAR,
            end_year=DTP_END_YEAR,
        )
        log.info("Данные ДТП успешно загружены")
    except Exception as e:
        log.exception(f"Ошибка при загрузке ДТП: {e}")
        return

    # ── 6. Трансформация ──────────────────────────────────────────────────────
    log.info("Шаг 6: Трансформация dtp_buffer → нормализованные таблицы")
    try:
        transform_dtp(
            fetch_batch_size=200,
            insert_chunk=10,
            resume=True,
        )
        log.info("Трансформация ДТП завершена")
    except Exception as e:
        log.exception(f"Ошибка при трансформации ДТП: {e}")
        return

    # ── 7. Загрузка погоды ────────────────────────────────────────────────────
    log.info(f"Шаг 7: Загрузка погоды ({WEATHER_START_YEAR}–{WEATHER_END_YEAR})")
    try:
        load_weather_for_cities(
            city_names=selected_cities,
            start_year=WEATHER_START_YEAR,
            end_year=WEATHER_END_YEAR,
        )
        log.info("Данные о погоде успешно загружены")
    except Exception as e:
        log.exception(f"Ошибка при загрузке погоды: {e}")
        return

    log.info("=" * 60)
    log.info("Пайплайн первоначальной загрузки завершён успешно")
    log.info("=" * 60)


if __name__ == "__main__":
    main()