"""
v2_load_dtp_buffer.py
---------------------
Версия 2. Загружает карточки ДТП с сайта ГИБДД (stat.gibdd.ru)
и сохраняет сырые данные в таблицу dtp_buffer в Supabase.


Основные точки входа:
    load_dtp_for_cities(city_names, json_path, start_year, end_year)
        — полная загрузка за диапазон лет (используется в v2_main.py)
    load_dtp_for_month(year, month, json_path)
        — загрузка только за один месяц (используется в updater.py)

Зависимости:
    pip install supabase python-dotenv requests

Переменные окружения (.env):
    SUPABASE_URL=https://xxxx.supabase.co
    SUPABASE_KEY=eyJ...   # service_role ключ
"""

import os
import json
import time
import logging
import requests
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

log = logging.getLogger(__name__)

_PREFIXES = ["МО пос.", "г.", "пгт.", "пос."]


# ─────────────────────────── Поиск по JSON ────────────────────────────────────

def _strip_prefix(name: str) -> str:
    """Убирает приставку из названия района/города."""
    for p in _PREFIXES:
        if name.startswith(p):
            return name[len(p):].strip()
    return name.strip()


def find_districts_by_city_names(
    city_names: list[str],
    json_path: str = "regions_all.json",
) -> list[dict]:
    """
    Ищет районы/города в regions_all.json по списку названий.
    При дублях предлагает выбор в консоли.
    """
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            regions_data = json.load(f)
    except FileNotFoundError:
        log.error(f"Файл не найден: {json_path}")
        return []

    index: dict[str, list[dict]] = {}
    for region in regions_data:
        for district in region.get("districts", []):
            clean = _strip_prefix(district["name"])
            entry = {
                "region_id":     region["id"],
                "region_name":   region["name"],
                "district_id":   district["id"],
                "district_name": district["name"],
            }
            index.setdefault(clean, []).append(entry)

    result = []

    for city in city_names:
        city_clean = city.strip()
        matches = index.get(city_clean, [])

        if not matches:
            log.warning(f"  '{city_clean}' не найден в regions_all.json — пропуск")
            continue

        if len(matches) == 1:
            result.append(matches[0])
            log.info(
                f"  '{city_clean}' → {matches[0]['region_name']} "
                f"(district_id={matches[0]['district_id']})"
            )
        else:
            print(f"\n⚠️  Город '{city_clean}' найден в нескольких регионах:")
            for i, m in enumerate(matches, 1):
                print(f"  {i}. {m['region_name']} — {m['district_name']}")
            print(f"  0. Пропустить '{city_clean}'")

            while True:
                raw = input(f"Выберите номер (1–{len(matches)}) или 0 для пропуска: ").strip()
                if raw == "0":
                    log.info(f"  '{city_clean}' пропущен пользователем")
                    break
                if raw.isdigit() and 1 <= int(raw) <= len(matches):
                    chosen = matches[int(raw) - 1]
                    result.append(chosen)
                    log.info(
                        f"  '{city_clean}' → {chosen['region_name']} "
                        f"(district_id={chosen['district_id']})"
                    )
                    break
                print(f"  Введите число от 0 до {len(matches)}")

    return result


# ─────────────────────────── Обновление даты в cities ─────────────────────────

def update_last_dtp_update(city_name: str):
    """
    Обновляет поле last_dtp_update в таблице cities для указанного города.
    Вызывается после успешной загрузки данных за месяц.
    """
    try:
        now = datetime.now(timezone.utc).isoformat()
        supabase.table("cities").update(
            {"last_dtp_update": now}
        ).eq("city_name", city_name).execute()
        log.info(f"  [{city_name}] last_dtp_update обновлён: {now}")
    except Exception as e:
        log.warning(f"  [{city_name}] Не удалось обновить last_dtp_update: {e}")


# ─────────────────────────── ГИБДД API ────────────────────────────────────────

def get_dtp_cards(region_id, district_id, year, month, start=1, end=100):
    """Получение полных карточек ДТП с пагинацией."""
    url = "http://stat.gibdd.ru/map/getDTPCardData"

    payload = {
        "data": {
            "date": [f"MONTHS:{month}.{year}"],
            "ParReg": region_id,
            "order": {"type": "1", "fieldName": "dat"},
            "reg": district_id,
            "ind": "1",
            "st": str(start),
            "en": str(end),
            "fil": {"isSummary": False},
            "fieldNames": [
                "dat", "time", "coordinates", "infoDtp", "k_ul", "dor", "ndu",
                "k_ts", "ts_info", "pdop", "pog", "osv", "s_pch", "s_pog",
                "n_p", "n_pg", "obst", "sdor", "t_osv", "t_p", "t_s", "v_p", "v_v"
            ]
        }
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip, deflate",
    }

    try:
        request_data = {"data": json.dumps(payload["data"], separators=(',', ':'))}
        response = requests.post(url, json=request_data, headers=headers, timeout=30)

        if response.status_code != 200:
            log.warning(f"HTTP {response.status_code} для {year}-{month:02d} start={start}")
            return None

        if not response.text.strip():
            log.warning(f"Пустой ответ для {year}-{month:02d} start={start}")
            return None

        try:
            outer_data = json.loads(response.text)
        except json.JSONDecodeError as e:
            log.error(f"Ошибка парсинга внешнего JSON: {e}")
            return None

        if "data" not in outer_data:
            log.warning(f"В ответе нет ключа 'data' для {year}-{month:02d}")
            return None

        inner_data_str = outer_data["data"]

        if not inner_data_str.strip():
            return []

        try:
            inner_data = json.loads(inner_data_str)
        except json.JSONDecodeError as e:
            log.error(f"Ошибка парсинга внутреннего JSON: {e}")
            return None

        return inner_data.get("tab", [])

    except requests.exceptions.RequestException as e:
        log.error(f"Ошибка сети/таймаут: {e}")
        return None
    except Exception as e:
        log.error(f"Непредвиденная ошибка: {e}")
        return None


# ─────────────────────────── Загрузка в Supabase ──────────────────────────────

def upload_to_supabase(records, region_id, region_name, district_id, district_name, year, month):
    log.info(f"Загрузка {len(records)} записей для {year}-{month:02d}")

    if not records:
        log.info("Нет записей — выходим")
        return

    prepared_records = []
    for record in records:
        dat_str  = record.get("dat", "")
        time_str = record.get("time", "")
        dtp_datetime = None
        dtp_date     = None
        if dat_str and time_str:
            try:
                dt = datetime.strptime(f"{dat_str} {time_str}", "%d.%m.%Y %H:%M")
                dt = dt.replace(tzinfo=ZoneInfo("Europe/Moscow"))
                dtp_datetime = dt.isoformat()
                dtp_date     = dt.date().isoformat()
            except Exception as e:
                log.warning(f"Ошибка парсинга даты: {e}")

        record_with_meta = {
            "region_id":     region_id,
            "region_name":   region_name,
            "district_id":   district_id,
            "district_name": district_name,
            "year":          year,
            "month":         month,
            "raw_data":      record,
            "dtp_datetime":  dtp_datetime,
            "dtp_date":      dtp_date,
        }

        try:
            json.dumps(record_with_meta)
        except TypeError as e:
            log.warning(f"Ошибка сериализации записи: {e}")
            continue

        prepared_records.append(record_with_meta)

    log.info(f"Подготовлено {len(prepared_records)} записей")

    chunk_size = 500
    for i in range(0, len(prepared_records), chunk_size):
        chunk = prepared_records[i:i + chunk_size]
        log.info(f"Вставляем чанк {i // chunk_size + 1}...")
        try:
            supabase.table("dtp_buffer").insert(chunk).execute()
            log.info(f"Чанк {i // chunk_size + 1} успешно вставлен")
        except Exception as e:
            log.error(f"Ошибка вставки чанка: {e}")
            raise

    log.info(f"✅ {year}-{month:02d}: загружено {len(prepared_records)} записей")

    # Проверка: подсчёт записей за этот месяц
    try:
        month_check = (
            supabase.table("dtp_buffer")
            .select("*", count="exact")
            .eq("region_id", region_id)
            .eq("year", year)
            .eq("month", month)
            .execute()
        )
        log.info(f"Всего записей для {year}-{month:02d} в таблице: {month_check.count}")
    except Exception as e:
        log.warning(f"Ошибка при подсчёте за месяц: {e}")


# ─────────────────────────── Пагинация по месяцу ──────────────────────────────

def fetch_all_for_month(region_id, region_name, district_id, district_name, year, month, retries=3):
    """Получает ВСЕ страницы данных для одного месяца и загружает в Supabase."""
    all_records = []
    page_size   = 100
    page        = 1

    while True:
        start = (page - 1) * page_size + 1
        end   = page * page_size

        for attempt in range(retries):
            dtp_data = get_dtp_cards(region_id, district_id, year, month, start, end)
            if dtp_data is not None:
                break
            log.warning(f"Повторная попытка {attempt + 1} для {year}-{month:02d} стр. {page}")
            time.sleep(2 ** attempt)
        else:
            log.error(f"Не удалось получить страницу {page} для {year}-{month:02d}, пропускаем")
            return False  # сигнал об ошибке

        if not dtp_data:
            break

        all_records.extend(dtp_data)

        if len(dtp_data) < page_size:
            break

        page += 1

    if all_records:
        upload_to_supabase(all_records, region_id, region_name,
                           district_id, district_name, year, month)
    else:
        log.info(f"⏺ {year}-{month:02d}: данных нет")

    return True  # успех (даже если данных не было — месяц обработан)


# ─────────────────────────── Основные точки входа ─────────────────────────────

def load_multiyear(region_id, region_name, district_id, district_name,
                   start_year: int, end_year: int):
    """
    Перебор всех месяцев в диапазоне лет для одного района.
    Диапазон задаётся явно — нет хардкода внутри функции.
    """
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            log.info(f"📅 Обработка {year}-{month:02d}...")
            fetch_all_for_month(region_id, region_name, district_id, district_name, year, month)


def load_dtp_for_cities(
    city_names: list[str],
    json_path:  str = "regions_all.json",
    start_year: int = 2015,
    end_year:   int = 2026,
):
    """
    Полная загрузка ДТП за диапазон лет.
    Используется в v2_main.py при первоначальной загрузке данных.

    После завершения загрузки для каждого города обновляет
    поле last_dtp_update в таблице cities.

    :param city_names: список названий городов (без приставок)
    :param json_path:  путь к regions_all.json
    :param start_year: начальный год
    :param end_year:   конечный год
    """
    log.info(f"Поиск районов для городов: {city_names}")
    districts = find_districts_by_city_names(city_names, json_path)

    if not districts:
        log.error("Не найдено ни одного района — загрузка ДТП отменена")
        return

    log.info(f"Найдено районов: {len(districts)}")

    for d in districts:
        log.info(
            f"\n🚗 Загружаем ДТП: {d['district_name']} ({d['region_name']}) "
            f"— {start_year}–{end_year}"
        )
        load_multiyear(
            region_id=d["region_id"],
            region_name=d["region_name"],
            district_id=d["district_id"],
            district_name=d["district_name"],
            start_year=start_year,
            end_year=end_year,
        )
        # Обновляем дату последней загрузки ДТП для города
        update_last_dtp_update(d["district_name"].lstrip("г. ").lstrip("пгт. ").strip())


def load_dtp_for_month(
    year:      int,
    month:     int,
    json_path: str = "regions_all.json",
):
    """
    Загрузка ДТП только за один конкретный месяц для всех городов из таблицы cities.
    Используется в updater.py при ежемесячном обновлении.

    После успешной загрузки обновляет last_dtp_update в таблице cities.

    :param year:      год
    :param month:     месяц (1–12)
    :param json_path: путь к regions_all.json
    """
    # Читаем города из рабочей таблицы cities
    try:
        resp = supabase.table("cities").select("city_name").execute()
        city_names = [row["city_name"] for row in resp.data]
    except Exception as e:
        log.error(f"Не удалось загрузить города из таблицы cities: {e}")
        return

    if not city_names:
        log.error("Таблица cities пуста — загрузка ДТП отменена")
        return

    log.info(f"Загрузка ДТП за {year}-{month:02d} для {len(city_names)} городов")
    districts = find_districts_by_city_names(city_names, json_path)

    if not districts:
        log.error("Не найдено ни одного района — загрузка ДТП отменена")
        return

    for d in districts:
        log.info(f"📅 [{d['district_name']}] {year}-{month:02d}...")
        success = fetch_all_for_month(
            region_id=d["region_id"],
            region_name=d["region_name"],
            district_id=d["district_id"],
            district_name=d["district_name"],
            year=year,
            month=month,
        )
        if success:
            city_clean = _strip_prefix(d["district_name"])
            update_last_dtp_update(city_clean)


# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    load_dtp_for_cities(
        city_names=["Лобня"],
        json_path="regions_all.json",
        start_year=2015,
        end_year=2026,
    )