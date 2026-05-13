"""
transform_dtp_buffer.py
-----------------------
Читает записи из dtp_buffer (поле raw_data) и загружает в 5 таблиц:
  - accidents          (основные данные о ДТП)
  - locations          (место, координаты, дорога)
  - vehicles           (транспортные средства)
  - participants       (участники из ts_info[].ts_uch[])
  - weather_conditions (погода и дорожные условия)

Реальная структура raw_data:
  Верхний уровень:
    KartId, date, Time, DTP_V, District,
    POG, RAN, K_TS, K_UCH, emtp_number, rowNum
  infoDtp (dict):
    n_p, street, house, km, m, dor, dor_k, dor_z,
    k_ul, ndu, sdor, s_dtp, s_pch, s_pog, osv,
    factor, OBJ_DTP, COORD_L, COORD_W,
    change_org_motion,
    ts_info  → список ТС
      каждое ТС: g_v, t_n, m_ts, n_ts, o_pf, t_ts, ts_s,
                 color, f_sob, m_pov, r_rul, marka_ts, ts_uch
        ts_uch → список участников
          каждый: POL, S_T, ALCO, NPDD, S_SM, V_ST,
                  K_UCH, N_UCH, SOP_NPDD, SAFETY_BELT,
                  S_SEAT_GROUP, INJURED_CARD_ID
    uchInfo (обычно пустой список)

Зависимости:
    pip install supabase python-dotenv

Переменные окружения (.env):
    SUPABASE_URL=https://xxxx.supabase.co
    SUPABASE_KEY=eyJ...   # service_role ключ
"""

import os
import json
import time
import logging
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ─────────────────────────── Подключение ──────────────────────────────────────

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # service_role key!

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ─────────────────────────── Прогресс ─────────────────────────────────────────

PROGRESS_FILE = "transform_progress.txt"

def load_progress() -> int:
    try:
        with open(PROGRESS_FILE) as f:
            return int(f.read().strip())
    except Exception:
        return 0

def save_progress(offset: int):
    with open(PROGRESS_FILE, "w") as f:
        f.write(str(offset))

def reset_progress():
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)


# ─────────────────────────── Вспомогательные функции ──────────────────────────

def to_int(v, default=None):
    try:
        return int(v) if v not in (None, "", "null") else default
    except (ValueError, TypeError):
        return default

def to_str(v):
    """Скалярное значение → строка. Список/словарь → JSON-строка."""
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    s = str(v).strip()
    return s if s else None

def to_date(v) -> str | None:
    """
    Конвертирует дату из формата ГИБДД (DD.MM.YYYY) в ISO (YYYY-MM-DD).
    PostgreSQL тип DATE принимает только ISO-формат через REST API.
    При некорректном значении возвращает None.
    """
    if not v:
        return None
    s = str(v).strip()
    try:
        from datetime import datetime as _dt
        return _dt.strptime(s, "%d.%m.%Y").date().isoformat()
    except ValueError:
        log.warning(f"Не удалось конвертировать дату: '{s}'")
        return None


# ─────────────────────────── Парсинг raw_data ─────────────────────────────────

def parse_record(raw: dict, district_name: str = None, region_name: str = None) -> dict:
    """
    Разбирает один словарь raw_data на 5 частей для загрузки в БД.

    :param district_name: эталонное название города из dtp_buffer.district_name.
        Используется как city_name в accidents и locations вместо сырых полей
        ГИБДД (District, n_p), которые непоследовательны между городами:
        для Астрахани District = район города, для Ставрополя = "г. Ставрополь".
    """
    info = raw.get("infoDtp") or {}
    if not isinstance(info, dict):
        info = {}

    kart_id = to_int(raw.get("KartId"))

    # ── accidents ──────────────────────────────────────────────────────────────
    accident = {
        "kart_id":     kart_id,
        "date":        to_date(raw.get("date")),    # DD.MM.YYYY → YYYY-MM-DD (тип DATE в БД)
        "time":        to_str(raw.get("Time")),
        "city_name":   district_name,               # надёжное название города из dtp_buffer
        "region_name": region_name,                 # надёжное название региона из dtp_buffer
        "district":    to_str(raw.get("District")), # оригинальное поле ГИБДД (непоследовательно)
        "dtp_type":    to_str(raw.get("DTP_V")),
        "pog":         to_int(raw.get("POG")),
        "ran":         to_int(raw.get("RAN")),
        "k_ts":        to_int(raw.get("K_TS")),
        "k_uch":       to_int(raw.get("K_UCH")),
        "emtp_number": to_str(raw.get("emtp_number")),
    }

    # ── locations ──────────────────────────────────────────────────────────────
    location = {
        "kart_id":       kart_id,
        "city_name":     district_name,              # надёжное название города из dtp_buffer
        "region_name":   region_name,                # надёжное название региона из dtp_buffer
        "city":          to_str(info.get("n_p")),    # оригинальное поле ГИБДД n_p (непоследовательно)
        "ndu":           to_str(info.get("ndu")),    # список → JSON-строка
        "sdor":          to_str(info.get("sdor")),   # список → JSON-строка
        "street":        to_str(info.get("street")),
        "house":         to_str(info.get("house")),
        "km":            to_str(info.get("km")),
        "m":             to_str(info.get("m")),
        "road_type":     to_str(info.get("k_ul")),
        "road_category": to_str(info.get("dor_k")),
        "coord_w":       to_str(info.get("COORD_W")),
        "coord_l":       to_str(info.get("COORD_L")),
        "objects_near":  to_str(info.get("OBJ_DTP")),  # список → JSON-строка
    }

    # ── vehicles ───────────────────────────────────────────────────────────────
    ts_list = info.get("ts_info") or []
    if not isinstance(ts_list, list):
        ts_list = []

    vehicles = []
    for ts in ts_list:
        if not isinstance(ts, dict):
            continue
        vehicles.append({
            "kart_id":    kart_id,
            "ts_number":  to_str(ts.get("n_ts")),
            "status":     to_str(ts.get("ts_s")),
            "class":      to_str(ts.get("t_ts")),
            "brand":      to_str(ts.get("marka_ts")),
            "model":      to_str(ts.get("m_ts")),
            "color":      to_str(ts.get("color")),
            "drive_type": to_str(ts.get("r_rul")),
            "year":       to_int(ts.get("g_v")),
            "defects":    to_str(ts.get("t_n")),
            "ownership":  to_str(ts.get("f_sob")),
            "owner_type": to_str(ts.get("o_pf")),
        })

    # ── participants (из ts_uch каждого ТС) ───────────────────────────────────
    participants = []
    for ts in ts_list:
        if not isinstance(ts, dict):
            continue
        ts_num = to_str(ts.get("n_ts"))
        for p in (ts.get("ts_uch") or []):
            if not isinstance(p, dict):
                continue
            participants.append({
                "kart_id":         kart_id,
                "n_ts":            ts_num,
                "n_uch":           to_str(p.get("N_UCH")),
                "k_uch":           to_str(p.get("K_UCH")),
                "pol":             to_str(p.get("POL")),
                "v_st":            to_str(p.get("V_ST")),
                "alco":            to_str(p.get("ALCO")),
                "s_t":             to_str(p.get("S_T")),
                "s_sm":            to_str(p.get("S_SM")),
                "safety_belt":     to_str(p.get("SAFETY_BELT")),
                "s_seat_group":    to_str(p.get("S_SEAT_GROUP")),
                "injured_card_id": to_str(p.get("INJURED_CARD_ID")),
            })

    # ── weather_conditions ─────────────────────────────────────────────────────
    weather = {
        "kart_id": kart_id,
        "pog":     to_str(info.get("s_pog")),   # список осадков → JSON-строка
        "osv":     to_str(info.get("osv")),
        "s_pch":   to_str(info.get("s_pch")),
        "s_pog":   to_str(info.get("s_pog")),
        "t_osv":   None,                         # поле отсутствует в данных
        "t_p":     to_str(info.get("s_dtp")),   # тип пересечения
        "t_s":     to_str(info.get("dor")),      # тип дороги
        "v_p":     to_str(info.get("factor")),   # сопутствующие факторы
        "v_v":     to_str(info.get("change_org_motion")),
        "obst":    to_str(info.get("dor_z")),
    }

    return {
        "accident":     accident,
        "location":     location,
        "vehicles":     vehicles,
        "participants": participants,
        "weather":      weather,
    }


# ─────────────────────────── Вставка через REST API ───────────────────────────

def sb_insert_chunk(table: str, rows: list, on_conflict: str = None, retries: int = 3):
    """Один HTTP-запрос. При ошибке — повтор с экспоненциальной задержкой."""
    if not rows:
        return
    for attempt in range(retries):
        try:
            if on_conflict:
                supabase.table(table).upsert(rows, on_conflict=on_conflict).execute()
            else:
                supabase.table(table).insert(rows).execute()
            return
        except Exception as e:
            wait = 2 ** attempt
            log.warning(f"  [{table}] попытка {attempt+1}/{retries}: {e}. Жду {wait}с...")
            time.sleep(wait)
    raise RuntimeError(f"Не удалось вставить в {table} после {retries} попыток")


def sb_insert_all(table: str, rows: list, on_conflict: str = None, chunk_size: int = 10):
    """Режет rows на чанки и вставляет каждый отдельным запросом."""
    for i in range(0, len(rows), chunk_size):
        sb_insert_chunk(table, rows[i:i + chunk_size], on_conflict=on_conflict)


def flush_buffers(acc, loc, veh, par, wea, chunk_size: int = 10):
    """Вставляет все 5 буферов. accidents — первым (FK-зависимости).

    Для accidents используется upsert с on_conflict по CONSTRAINT
    accidents_kart_id_district_key (kart_id, district).
    Это позволяет безопасно перезапускать трансформацию без дублей —
    повторная вставка той же записи просто обновит её.
    """
    if not acc:
        return
    sb_insert_all("accidents",          acc, on_conflict="kart_id,district", chunk_size=chunk_size)
    sb_insert_all("locations",          loc, chunk_size=chunk_size)
    sb_insert_all("vehicles",           veh, chunk_size=chunk_size)
    sb_insert_all("participants",       par, chunk_size=chunk_size)
    sb_insert_all("weather_conditions", wea, chunk_size=chunk_size)


# ─────────────────────────── Основной цикл ────────────────────────────────────

def transform_and_load(
    fetch_batch_size: int = 200,
    insert_chunk:     int = 10,
    resume:           bool = True,
    min_id:           int = 0,
):
    """
    :param min_id: обрабатывать только записи dtp_buffer с id > min_id.
                   0 = все записи (полная загрузка через v2_main.py).
                   Передаётся из updater.py для обработки только новых записей.
    """
    total_ok      = 0
    total_skipped = 0
    offset = load_progress() if resume else 0

    if offset > 0:
        log.info(f"▶️  Продолжаем с offset={offset}")
    log.info(f"Старт: fetch={fetch_batch_size}, chunk={insert_chunk}")

    while True:
        # ── Читаем страницу из dtp_buffer ──────────────────────────────────────
        query = (
            supabase.table("dtp_buffer")
            .select("id, raw_data, district_name, region_name")  # из dtp_buffer — надёжнее чем raw_data["District"]
            .order("id")
        )
        if min_id > 0:
            query = query.gt("id", min_id)
        resp = query.range(offset, offset + fetch_batch_size - 1).execute()
        rows = resp.data
        if not rows:
            log.info("✅ Все записи обработаны.")
            reset_progress()
            break

        log.info(f"Батч {offset}–{offset + len(rows) - 1} ({len(rows)} записей) ...")

        # ── Парсим ─────────────────────────────────────────────────────────────
        acc_buf, loc_buf, veh_buf, par_buf, wea_buf = [], [], [], [], []
        parse_errors = 0

        for row in rows:
            raw = row.get("raw_data")
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except json.JSONDecodeError:
                    log.warning(f"  JSON-ошибка, buffer id={row['id']}")
                    parse_errors += 1
                    continue
            if not isinstance(raw, dict):
                parse_errors += 1
                continue
            try:
                district_name = row.get("district_name")
                region_name   = row.get("region_name")
                p = parse_record(raw, district_name=district_name, region_name=region_name)
            except Exception as e:
                log.warning(f"  Ошибка парсинга id={row['id']}: {e}")
                parse_errors += 1
                continue
            if p["accident"]["kart_id"] is None:
                log.warning(f"  Нет KartId, buffer id={row['id']}, пропуск")
                parse_errors += 1
                continue

            acc_buf.append(p["accident"])
            loc_buf.append(p["location"])
            veh_buf.extend(p["vehicles"])
            par_buf.extend(p["participants"])
            wea_buf.append(p["weather"])

        # ── Вставляем мини-батчами ──────────────────────────────────────────────
        insert_ok  = 0
        insert_err = 0
        try:
            flush_buffers(acc_buf, loc_buf, veh_buf, par_buf, wea_buf, chunk_size=insert_chunk)
            insert_ok = len(acc_buf)
        except Exception as e:
            log.error(f"  ❌ Ошибка вставки батча: {e}")
            log.info("  🔄 Фолбэк: вставка по 1 записи...")
            for i in range(len(acc_buf)):
                try:
                    flush_buffers(
                        [acc_buf[i]], [loc_buf[i]],
                        [v for v in veh_buf if v["kart_id"] == acc_buf[i]["kart_id"]],
                        [p for p in par_buf if p["kart_id"] == acc_buf[i]["kart_id"]],
                        [wea_buf[i]],
                        chunk_size=1,
                    )
                    insert_ok += 1
                except Exception as e2:
                    log.warning(f"  ❌ Пропуск kart_id={acc_buf[i]['kart_id']}: {e2}")
                    insert_err += 1

        total_ok      += insert_ok
        total_skipped += parse_errors + insert_err
        offset        += fetch_batch_size

        log.info(
            f"  ✅ вставлено: {insert_ok} | "
            f"ошибок: {parse_errors + insert_err} | "
            f"всего: {total_ok}"
        )
        save_progress(offset)

    log.info(f"Готово. Загружено: {total_ok}, пропущено: {total_skipped}")


# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    transform_and_load(
        fetch_batch_size=200,  # строк читаем из dtp_buffer за раз
        insert_chunk=10,       # строк в одном INSERT (5 = надёжнее, 20 = быстрее)
        resume=False,          # False = начать сначала, True = продолжить после сбоя
    )