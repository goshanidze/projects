# DTP Analytics Pipeline

ETL-пайплайн для сбора, нормализации и анализа данных о дорожно-транспортных происшествиях (ДТП) по городам России. Данные загружаются с сайта ГИБДД (stat.gibdd.ru) и обогащаются архивными метеоданными (Open-Meteo). Результат хранится в Supabase и визуализируется в Yandex DataLens.

---

## Стек

| Компонент | Технология |
|---|---|
| Язык | Python 3.11+ |
| База данных | Supabase (PostgreSQL) |
| Источник ДТП | stat.gibdd.ru (ГИБДД) |
| Источник погоды | Open-Meteo Archive API |
| Геокодирование | Yandex Geocoder API |
| Визуализация | Yandex DataLens |

---

## Переменные окружения (.env)

```
SUPABASE_URL=https://xxxx.supabase.co
SUPABASE_KEY=eyJ...          # service_role ключ
SUPABASE_DB_URL=postgresql://postgres:[PASSWORD]@db.[REF].supabase.co:5432/postgres
YANDEX_APIKEY=...            # Yandex Geocoder API key
```

---

## Структура базы данных

### Буферные таблицы (сырые данные)

| Таблица | Описание |
|---|---|
| `dtp_buffer` | Сырые карточки ДТП с сайта ГИБДД в формате JSONB |
| `cities_buffer` | Полный справочник городов России с населением > 100 000 человек |

### Нормализованные таблицы ДТП

| Таблица | Описание |
|---|---|
| `accidents` | Основные данные о ДТП: дата, тип, погибшие, раненые, город, регион |
| `locations` | Место ДТП: улица, координаты, тип и категория дороги |
| `vehicles` | Транспортные средства: марка, модель, тип, год выпуска |
| `participants` | Участники ДТП: пол, роль, алкоголь, ремень, тяжесть травм |
| `weather_conditions` | Условия на месте ДТП по оценке инспектора ГИБДД |

### Прочие таблицы

| Таблица | Описание |
|---|---|
| `weather_history` | Архивные метеоданные по городам за 10 лет (Open-Meteo) |
| `cities` | Рабочий набор городов для мониторинга + даты последних обновлений |

### Ключевые связи

- `accidents.kart_id` → `locations`, `vehicles`, `participants`, `weather_conditions`
- `accidents.city_name` + `accidents.date` → `weather_history.city_name` + `weather_history.time`
- Уникальность в `accidents` обеспечивается CONSTRAINT `(kart_id, district)` — `kart_id` не глобально уникален, только внутри одного района ГИБДД

---

## Файлы проекта

### `create_target_tables.py`
Создаёт все таблицы в Supabase. Запускается однократно перед первым запуском пайплайна. Использует `psycopg2` для прямого подключения к БД (через `SUPABASE_DB_URL`). Содержит `DO $$ BEGIN ... END $$` блок для безопасного повторного запуска — уже существующие таблицы и constraint-ы не пересоздаются.

### `load_city_buffer.py`
Загружает список городов России из Википедии (страница «Список городов России»), геокодирует их через Yandex Geocoder API и сохраняет в таблицу `cities_buffer`. Парсинг HTML выполняется через BeautifulSoup, фильтрация — города с населением > 100 000 человек.

### `load_dtp_buffer.py`
Загружает сырые карточки ДТП с сайта stat.gibdd.ru и сохраняет в таблицу `dtp_buffer`. Ищет города в файле `regions_all.json` с автоматическим снятием приставок («г.», «пгт.», «пос.», «МО пос.»). При совпадении названия города в нескольких регионах предлагает пользователю выбрать нужный в консоли. После успешной загрузки обновляет поле `last_dtp_update` в таблице `cities`. Поддерживает два режима: полная загрузка за диапазон лет (`load_dtp_for_cities`) и загрузка только за один месяц (`load_dtp_for_month`).

### `transform_dtp_buffer.py`
Читает сырые записи из `dtp_buffer` и раскладывает их в 5 нормализованных таблиц: `accidents`, `locations`, `vehicles`, `participants`, `weather_conditions`. Поддерживает возобновление после сбоя через файл прогресса `transform_progress.txt`. Вставка идёт мини-батчами (по умолчанию 10 строк) для обхода ограничений Supabase на timeout. Параметр `min_id` позволяет обрабатывать только новые записи (используется в `updater.py`). Дата ДТП конвертируется из формата ГИБДД `DD.MM.YYYY` в ISO `YYYY-MM-DD` для корректного хранения в типе `DATE`.

### `load_weather_buffer.py`
Загружает архивные метеоданные из Open-Meteo Archive API за указанный период и сохраняет в таблицу `weather_history`. Читает города из таблицы `cities`. Координаты уточняются через Open-Meteo Geocoding API (не Yandex) для точного соответствия сетке архивных данных. Поддерживает два режима: полная загрузка за диапазон лет (`load_weather_for_cities`) и загрузка только за один месяц (`load_weather_for_last_month`). После успешной загрузки обновляет поле `last_weather_update` в таблице `cities`.

### `regions_all.json`
Справочник регионов и районов России для API ГИБДД. Структура: регион (`id`, `name`) → список районов/городов (`id`, `name`). Используется в `load_dtp_buffer.py` для сопоставления названий городов с идентификаторами API ГИБДД (`ParReg`, `reg`).

### `inspect_raw_data.py`
Вспомогательный скрипт для изучения структуры `raw_data` в таблице `dtp_buffer`. Выводит ключи верхнего уровня, структуру `infoDtp`, `ts_info` и `ts_uch`. Используется при отладке маппинга полей.

### `inspect_wiki_tables.py`
Вспомогательный скрипт для диагностики HTML-структуры страницы Википедии со списком городов. Находит все классы таблиц на странице. Использовался при отладке парсинга в `load_city_buffer.py`.

---

## main.py

Скрипт первоначальной загрузки данных. Запускается **однократно** для инициализации проекта.

**Временной диапазон** вычисляется автоматически на основе даты запуска:
- `END_YEAR` = текущий год
- `START_YEAR` = текущий год − 10

**Пайплайн:**

```
Шаг 0 — создание таблиц (create_target_tables)
Шаг 1 — загрузка городов из Википедии (load_city_buffer)
Шаг 2 — геокодирование через Яндекс (load_city_buffer)
Шаг 3 — сохранение в cities_buffer (load_city_buffer)
Шаг 4 — выбор городов пользователем + копирование в таблицу cities
Шаг 5 — загрузка ДТП за START_YEAR–END_YEAR (load_dtp_buffer)
Шаг 6 — трансформация dtp_buffer → нормализованные таблицы (transform_dtp_buffer)
Шаг 7 — загрузка погоды за START_YEAR–END_YEAR (load_weather_buffer)
```

На шаге 4 пользователь вводит названия городов через запятую. Каждый город проверяется по таблице `cities_buffer`. Пустой ввод — загружаются все города. Выбранные города копируются из `cities_buffer` в `cities` (рабочий набор).

Логирование ведётся одновременно в консоль и файл `main_v2.log`.


## updater.py

Скрипт ежемесячного инкрементального обновления. Запускается **автоматически по расписанию** — не требует участия пользователя.

**Логика защиты от дублей:** перед загрузкой каждого источника проверяется поле `last_dtp_update` / `last_weather_update` в таблице `cities`. Если с момента последнего обновления прошло менее `MIN_UPDATE_DAYS` (28 дней) — загрузка для этого города пропускается.

**Инкрементальная трансформация:** перед загрузкой новых данных в `dtp_buffer` фиксируется текущий максимальный `id` (`get_max_dtp_buffer_id`). После загрузки трансформация запускается с параметром `min_id` — обрабатываются только новые записи, уже трансформированные данные не затрагиваются.

**Пайплайн:**

```
Фиксируем max id в dtp_buffer (до загрузки)
Шаг 1 — загрузка ДТП за прошлый месяц (load_dtp_buffer.load_dtp_for_month)
Шаг 2 — трансформация только новых записей (transform_dtp_buffer, min_id=max_id_before)
Шаг 3 — загрузка погоды за прошлый месяц (load_weather_buffer.load_weather_for_last_month)
```

Логирование ведётся в консоль и файл `updater.log`.

**Настройка автозапуска (GitHub Actions):**
```yaml
on:
  schedule:
    - cron: '0 3 1 * *'  # 1-го числа каждого месяца в 3:00 UTC
```

**Настройка автозапуска (Windows Task Scheduler):**
```
Программа: python
Аргументы: D:\myproj\workshop\updater.py
Расписание: ежемесячно, 1-го числа
```

**Импорты:**
```python
from load_dtp_buffer     import load_dtp_for_month
from load_weather_buffer import load_weather_for_last_month
from transform_dtp_buffer   import transform_and_load
```

---

## Датасеты для Yandex DataLens

### Датасет 1 — ДТП + архивная погода
```sql
accidents
  LEFT JOIN weather_history
      ON accidents.city_name = weather_history.city_name
     AND accidents.date = weather_history.time
```
Анализ зависимости ДТП от погодных условий, динамика по годам и сезонам, сравнение городов.

### Датасет 2 — ДТП + место + условия на дороге
```sql
accidents
  LEFT JOIN locations        ON accidents.kart_id = locations.kart_id
  LEFT JOIN weather_conditions ON accidents.kart_id = weather_conditions.kart_id
```
Анализ по типу дороги, покрытию и освещённости.

### Датасет 3 — Участники и транспорт
```sql
accidents
  LEFT JOIN vehicles     ON accidents.kart_id = vehicles.kart_id
  LEFT JOIN participants ON accidents.kart_id = participants.kart_id
```
Профиль участников, статистика по маркам и типам ТС.
