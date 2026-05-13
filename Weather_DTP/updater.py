"""
updater.py
----------
Скрипт ежемесячного инкрементального обновления данных.
Запускается автоматически по расписанию (раз в месяц).

Логика работы:
    1. Вычисляет прошлый месяц автоматически.
    2. Для каждого города из таблицы cities проверяет даты
       последних обновлений (last_dtp_update, last_weather_update).
    3. Если с момента последнего обновления прошло меньше MIN_UPDATE_DAYS —
       загрузка для этого источника пропускается (защита от дублей).
    4. Загружает только данные за прошлый месяц, не трогая историю.
    5. После успешной загрузки обновляет last_*_update в таблице cities.
    6. После загрузки ДТП запускает трансформацию dtp_buffer.

Настройка автозапуска (GitHub Actions, .github/workflows/updater.yml):
    on:
      schedule:
        - cron: '0 3 1 * *'  # 1-го числа каждого месяца в 3:00 UTC

Настройка автозапуска (Windows Task Scheduler):
    Программа: python
    Аргументы: D:\\myproj\\workshop\\updater.py
    Расписание: ежемесячно, 1-го числа

Переменные окружения (.env):
    SUPABASE_URL=https://xxxx.supabase.co
    SUPABASE_KEY=eyJ...   # service_role ключ
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client

from v2_load_dtp_buffer import load_dtp_for_month
from v2_load_weather_buffer import load_weather_for_last_month
from transform_dtp_buffer import transform_and_load as transform_dtp

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Минимальный интервал между обновлениями в днях.
# Если последнее обновление было менее MIN_UPDATE_DAYS назад — пропускаем.
# Защищает от дублей при повторном запуске в том же месяце.
MIN_UPDATE_DAYS = 28

# ─────────────────────────── Логирование ──────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("updater.log", encoding="utf-8"),
    ]
)
log = logging.getLogger(__name__)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ─────────────────────────── Вспомогательные функции ──────────────────────────

def get_last_month() -> tuple[int, int]:
    """
    Возвращает (год, месяц) прошлого месяца.
    Например, если сегодня март 2026 — вернёт (2026, 2).
    """
    today = datetime.now(timezone.utc)
    first_of_month = today.replace(day=1)
    last_month     = first_of_month - timedelta(days=1)
    return last_month.year, last_month.month


def days_since(dt_str: str | None) -> float:
    """
    Возвращает количество дней с момента dt_str (ISO-строка с TZ) до сейчас.
    Если dt_str is None — возвращает бесконечность (обновление никогда не было).
    """
    if dt_str is None:
        return float("inf")
    try:
        dt  = datetime.fromisoformat(dt_str)
        now = datetime.now(timezone.utc)
        # Если dt без TZ — добавляем UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (now - dt).total_seconds() / 86400
    except Exception:
        return float("inf")


def fetch_cities_with_update_dates() -> list[dict]:
    """
    Загружает все города из таблицы cities вместе с датами последних обновлений.
    """
    try:
        resp = supabase.table("cities").select(
            "city_name, last_dtp_update, last_weather_update"
        ).execute()
        return resp.data or []
    except Exception as e:
        log.error(f"Не удалось загрузить города из таблицы cities: {e}")
        return []


def check_needs_update(city: dict, source: str) -> bool:
    """
    Проверяет, нужно ли обновлять данные для города.

    :param city:   строка из таблицы cities
    :param source: 'dtp' или 'weather'
    :return: True — нужно обновить, False — пропустить
    """
    field    = "last_dtp_update" if source == "dtp" else "last_weather_update"
    last_upd = city.get(field)
    days     = days_since(last_upd)

    if days < MIN_UPDATE_DAYS:
        log.info(
            f"  [{city['city_name']}] {source}: последнее обновление "
            f"{days:.1f} дн. назад — пропуск (минимум {MIN_UPDATE_DAYS} дн.)"
        )
        return False

    return True


# ─────────────────────────── Основная функция ─────────────────────────────────

def get_max_dtp_buffer_id() -> int:
    """
    Возвращает максимальный id в dtp_buffer на текущий момент.
    Вызывается ДО загрузки новых данных — чтобы трансформация
    обработала только записи с id > этого значения.
    """
    try:
        resp = supabase.table("dtp_buffer").select("id").order("id", desc=True).limit(1).execute()
        if resp.data:
            return resp.data[0]["id"]
        return 0
    except Exception as e:
        log.warning(f"Не удалось получить max id из dtp_buffer: {e}")
        return 0


def run_update():
    year, month = get_last_month()

    log.info("=" * 60)
    log.info(f"updater — обновление данных за {year}-{month:02d}")
    log.info("=" * 60)

    cities = fetch_cities_with_update_dates()

    if not cities:
        log.error("Таблица cities пуста — обновление невозможно. "
                  "Запустите v2_main.py для первоначальной загрузки.")
        return

    log.info(f"Городов в рабочем наборе: {len(cities)}")

    # ── Определяем, каким городам нужно обновление ────────────────────────────
    cities_need_dtp     = [c for c in cities if check_needs_update(c, "dtp")]
    cities_need_weather = [c for c in cities if check_needs_update(c, "weather")]

    log.info(f"Нуждаются в обновлении ДТП:    {len(cities_need_dtp)} городов")
    log.info(f"Нуждаются в обновлении погоды: {len(cities_need_weather)} городов")

    # ── Фиксируем текущий максимальный id в dtp_buffer ───────────────────────
    # Делаем это ДО загрузки новых данных — трансформация обработает
    # только записи с id > этого значения, не трогая уже трансформированные.
    max_id_before = get_max_dtp_buffer_id()
    log.info(f"Текущий max id в dtp_buffer: {max_id_before}")

    # ── 1. Загрузка ДТП за прошлый месяц ─────────────────────────────────────
    if cities_need_dtp:
        log.info(f"Шаг 1: Загрузка ДТП за {year}-{month:02d}")
        try:
            load_dtp_for_month(
                year=year,
                month=month,
                json_path="regions_all.json",
            )
            log.info("Загрузка ДТП за месяц завершена")
        except Exception as e:
            log.exception(f"Ошибка при загрузке ДТП: {e}")
    else:
        log.info("Шаг 1: Все города уже обновлены по ДТП — пропуск")

    # ── 2. Трансформация новых записей из dtp_buffer ──────────────────────────
    if cities_need_dtp:
        log.info("Шаг 2: Трансформация новых записей dtp_buffer")
        try:
            transform_dtp(
                fetch_batch_size=200,
                insert_chunk=10,
                resume=False,          # для инкрементальной загрузки всегда False
                min_id=max_id_before,  # обрабатываем только новые записи
            )
            log.info("Трансформация завершена")
        except Exception as e:
            log.exception(f"Ошибка при трансформации ДТП: {e}")
    else:
        log.info("Шаг 2: Трансформация не нужна — пропуск")

    # ── 3. Загрузка погоды за прошлый месяц ──────────────────────────────────
    if cities_need_weather:
        log.info(f"Шаг 3: Загрузка погоды за {year}-{month:02d}")
        try:
            load_weather_for_last_month(year=year, month=month)
            log.info("Загрузка погоды за месяц завершена")
        except Exception as e:
            log.exception(f"Ошибка при загрузке погоды: {e}")
    else:
        log.info("Шаг 3: Все города уже обновлены по погоде — пропуск")

    log.info("=" * 60)
    log.info("Обновление завершено")
    log.info("=" * 60)


if __name__ == "__main__":
    run_update()