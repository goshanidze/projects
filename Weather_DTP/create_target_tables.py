"""
v2_create_target_tables.py
--------------------------
Версия 2. Создаёт все необходимые таблицы в базе данных Supabase.
Запускать однократно перед первым запуском пайплайна.


Таблицы:
    accidents          — основные данные о ДТП
    locations          — место ДТП, координаты, дорога
    vehicles           — транспортные средства
    participants       — участники ДТП
    weather_conditions — погода и дорожные условия на месте ДТП
    weather_history    — архивные данные о погоде по городам (Open-Meteo)
    dtp_buffer         — сырые карточки ДТП с ГИБДД
    cities_buffer      — все города России с населением > 100 000 человек
    cities             — рабочий набор городов + даты последних обновлений

Зависимости:
    pip install psycopg2-binary python-dotenv

Переменные окружения (.env):
    SUPABASE_DB_URL=postgresql://postgres:[PASSWORD]@db.[REF].supabase.co:5432/postgres
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("SUPABASE_DB_URL")


def create_tables():
    sql = """
    -- ── Нормализованные таблицы ДТП ───────────────────────────────────────────

    CREATE TABLE IF NOT EXISTS accidents (
        id          BIGSERIAL PRIMARY KEY,           -- суррогатный ключ
        kart_id     BIGINT NOT NULL,                 -- ID карточки ГИБДД (не уникален между регионами!)
        date        DATE,
        time        TEXT,
        city_name   TEXT,                            -- надёжное название города из dtp_buffer (для JOIN с weather_history)
        region_name TEXT,                            -- надёжное название региона из dtp_buffer
        district    TEXT,                            -- оригинальное поле ГИБДД (район или город — непоследовательно)
        dtp_type    TEXT,
        pog         INTEGER,
        ran         INTEGER,
        k_ts        INTEGER,
        k_uch       INTEGER,
        emtp_number TEXT,
        created_at  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS locations (
        id            BIGSERIAL PRIMARY KEY,
        kart_id       BIGINT,
        city_name     TEXT,                          -- надёжное название города из dtp_buffer (для JOIN с weather_history)
        region_name   TEXT,                          -- надёжное название региона из dtp_buffer
        city          TEXT,                          -- оригинальное поле ГИБДД n_p (непоследовательно)
        ndu           TEXT,
        sdor          TEXT,
        street        TEXT,
        house         TEXT,
        km            TEXT,
        m             TEXT,
        road_type     TEXT,
        road_category TEXT,
        coord_w       TEXT,
        coord_l       TEXT,
        objects_near  TEXT,
        created_at    TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS vehicles (
        id          BIGSERIAL PRIMARY KEY,
        kart_id     BIGINT,
        ts_number   TEXT,
        status      TEXT,
        class       TEXT,
        brand       TEXT,
        model       TEXT,
        color       TEXT,
        drive_type  TEXT,
        year        INTEGER,
        defects     TEXT,
        ownership   TEXT,
        owner_type  TEXT,
        created_at  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS participants (
        id              BIGSERIAL PRIMARY KEY,
        kart_id         BIGINT,
        n_ts            TEXT,
        n_uch           TEXT,
        k_uch           TEXT,
        pol             TEXT,
        v_st            TEXT,
        alco            TEXT,
        s_t             TEXT,
        s_sm            TEXT,
        safety_belt     TEXT,
        s_seat_group    TEXT,
        injured_card_id TEXT,
        created_at      TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS weather_conditions (
        id         BIGSERIAL PRIMARY KEY,
        kart_id    BIGINT,
        pog        TEXT,
        osv        TEXT,
        s_pch      TEXT,
        s_pog      TEXT,
        t_osv      TEXT,
        t_p        TEXT,
        t_s        TEXT,
        v_p        TEXT,
        v_v        TEXT,
        obst       TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Составной UNIQUE CONSTRAINT на accidents — PostgREST требует именно CONSTRAINT
    -- (не просто INDEX) для upsert on_conflict. Обёртка DO $$ позволяет
    -- пропустить создание если constraint уже существует (безопасный повторный запуск).
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'accidents_kart_id_district_key'
        ) THEN
            ALTER TABLE accidents
                ADD CONSTRAINT accidents_kart_id_district_key UNIQUE (kart_id, district);
        END IF;
    END $$;

    -- Индексы для быстрых JOIN-ов по kart_id в зависимых таблицах
    CREATE INDEX IF NOT EXISTS idx_locations_kart_id    ON locations          (kart_id);
    CREATE INDEX IF NOT EXISTS idx_vehicles_kart_id     ON vehicles           (kart_id);
    CREATE INDEX IF NOT EXISTS idx_participants_kart_id ON participants        (kart_id);
    CREATE INDEX IF NOT EXISTS idx_weather_cond_kart_id ON weather_conditions  (kart_id);

    -- ── Исторические данные погоды ─────────────────────────────────────────────

    CREATE TABLE IF NOT EXISTS weather_history (
        id                          SERIAL PRIMARY KEY,
        time                        DATE NOT NULL,
        city_name                   TEXT NOT NULL,
        latitude                    DOUBLE PRECISION,
        longitude                   DOUBLE PRECISION,
        temperature_2m_max          DOUBLE PRECISION,
        temperature_2m_min          DOUBLE PRECISION,
        temperature_2m_mean         DOUBLE PRECISION,
        apparent_temperature_max    DOUBLE PRECISION,
        apparent_temperature_min    DOUBLE PRECISION,
        apparent_temperature_mean   DOUBLE PRECISION,
        precipitation_sum           DOUBLE PRECISION,
        rain_sum                    DOUBLE PRECISION,
        snowfall_sum                DOUBLE PRECISION,
        precipitation_hours         DOUBLE PRECISION,
        weather_code                INTEGER,
        wind_speed_10m_max          DOUBLE PRECISION,
        wind_gusts_10m_max          DOUBLE PRECISION,
        wind_direction_10m_dominant DOUBLE PRECISION,
        shortwave_radiation_sum     DOUBLE PRECISION,
        et0_fao_evapotranspiration  DOUBLE PRECISION,
        sunrise                     TIMESTAMP,
        sunset                      TIMESTAMP,
        daylight_duration           DOUBLE PRECISION,
        sunshine_duration           DOUBLE PRECISION,
        year                        INTEGER,
        month                       INTEGER,
        season                      TEXT,
        created_at                  TIMESTAMP DEFAULT NOW()
    );

    CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_city_date
        ON weather_history (city_name, time);

    -- ── Буферные таблицы ───────────────────────────────────────────────────────

    CREATE TABLE IF NOT EXISTS dtp_buffer (
        id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        region_id     TEXT,
        district_id   TEXT,
        year          SMALLINT,
        month         SMALLINT,
        raw_data      JSONB,
        created_at    TIMESTAMPTZ DEFAULT NOW(),
        region_name   TEXT,
        district_name TEXT,
        dtp_date      DATE,
        dtp_datetime  TIMESTAMPTZ
    );

    -- Полный справочник городов России с населением > 100 000 человек
    CREATE TABLE IF NOT EXISTS cities_buffer (
        id         BIGSERIAL PRIMARY KEY,
        city_name  TEXT NOT NULL UNIQUE,
        region     TEXT,
        federal    TEXT,
        population INTEGER,
        lat        DOUBLE PRECISION,
        lon        DOUBLE PRECISION,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- ── Рабочий набор городов (v2) ─────────────────────────────────────────────
    -- Копируется из cities_buffer при первом запуске main.py.
    -- last_dtp_update и last_weather_update используются в updater.py
    -- для проверки: если разница с текущей датой < 30 дней — пропустить загрузку.

    CREATE TABLE IF NOT EXISTS cities (
        id                  BIGSERIAL PRIMARY KEY,
        city_name           TEXT NOT NULL UNIQUE,
        region              TEXT,
        federal             TEXT,
        population          INTEGER,
        lat                 DOUBLE PRECISION,
        lon                 DOUBLE PRECISION,
        last_dtp_update     TIMESTAMPTZ,   -- когда последний раз загружались ДТП
        last_weather_update TIMESTAMPTZ,   -- когда последний раз загружалась погода
        added_at            TIMESTAMPTZ DEFAULT NOW()
    );
    """

    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        print("✅ Таблицы успешно созданы (или уже существуют).")
    except Exception as e:
        print(f"❌ Ошибка при создании таблиц: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    create_tables()