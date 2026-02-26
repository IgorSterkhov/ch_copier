# ClickHouse Migration Tool

GUI-приложение на Python/Tkinter для миграции таблиц и данных между двумя ClickHouse инстансами.

## Возможности

- Подключение к source и destination ClickHouse (HTTP 8123, HTTPS 8443, Native 9000)
- Просмотр схемы source: базы данных, таблицы, views, dictionaries
- Просмотр DDL таблиц (SHOW CREATE TABLE / SHOW CREATE DICTIONARY)
- Выбор таблиц для миграции через чекбоксы в дереве схемы
- Генерация SELECT-запросов с фильтром по дате и LIMIT
- Генерация DDL для destination с автоматической очисткой Replicated*MergeTree ENGINE
- Создание таблиц на destination (CREATE OR REPLACE TABLE)
- Миграция данных с прогрессом и обработкой ошибок
- Запуск ClickHouse в Docker-контейнере прямо из GUI как destination
- Копирование SQL/DDL в буфер обмена

## Требования

- Python 3.10+
- tkinter (`sudo apt install python3-tk`)
- Docker (опционально, для запуска destination в контейнере)

## Установка

```bash
# Клонировать репозиторий
git clone <url>
cd ch_copy

# Создать виртуальное окружение
python3 -m venv venv
source venv/bin/activate

# Установить зависимости
pip install -r requirements.txt

# Создать конфигурацию
cp .env.example .env
```

## Настройка .env

Отредактируйте `.env` файл, указав параметры подключения:

```ini
# Source ClickHouse
SOURCE_HOST=source-ch.example.com
SOURCE_PORT=8123
SOURCE_USER=default
SOURCE_PASS=my_password
SOURCE_DB=default
SOURCE_SECURE=False
SOURCE_CA_CERT=

# Destination ClickHouse
DESTINATION_HOST=dest-ch.example.com
DESTINATION_PORT=8123
DESTINATION_USER=default
DESTINATION_PASS=
DESTINATION_DB=default
DESTINATION_SECURE=False
DESTINATION_CA_CERT=
```

| Параметр | Описание |
|---|---|
| `*_HOST` | Хост ClickHouse |
| `*_PORT` | Порт: `8123` (HTTP), `8443` (HTTPS), `9000` (Native) |
| `*_USER` | Имя пользователя |
| `*_PASS` | Пароль |
| `*_DB` | База данных по умолчанию |
| `*_SECURE` | SSL/TLS: `True` или `False` |
| `*_CA_CERT` | Путь к CA-сертификату для SSL (опционально) |

## Запуск

```bash
source venv/bin/activate
python ch_migrate.py
```

## Использование

### 1. Подключение

- Нажмите **"Подключиться"** в панели **Source** — приложение подключится к source и загрузит дерево схемы.
- Нажмите **"Подключиться"** в панели **Destination** для подключения к destination из `.env`.
- Или нажмите **"Docker CH"** для запуска нового ClickHouse-контейнера как destination.

### 2. Docker CH (опционально)

Если destination ещё нет, можно поднять его в Docker прямо из GUI:

1. Нажмите **"Docker CH"** в панели Destination.
2. В диалоге укажите имя контейнера, порт, образ и пароль (опционально).
3. Нажмите **"Запустить"** — контейнер поднимется и приложение автоматически подключится к нему.
4. Для остановки нажмите **"Остановить Docker"**.

### 3. Выбор таблиц

- Раскройте базу данных в дереве слева.
- Кликните на таблицу — появится её DDL в области превью, а рядом с именем переключится чекбокс.
- Выберите нужные таблицы из разных баз данных.

### 4. Генерация SQL

- Настройте фильтры: выберите колонку даты, укажите дату и/или LIMIT.
- Нажмите **"Сгенерировать SELECT"** — в текстовом поле появятся редактируемые SQL-запросы.
- При необходимости отредактируйте запросы вручную.

### 5. Генерация и создание DDL

1. Нажмите **"Генерировать DDL"** — приложение создаст DDL-скрипты:
   - `CREATE DATABASE IF NOT EXISTS` для каждой БД.
   - `CREATE OR REPLACE TABLE` с очисткой Replicated*MergeTree ENGINE (удаление аргументов ZooKeeper).
2. Отредактируйте DDL при необходимости.
3. Нажмите **"Создать DDL на Destination"** — скрипты выполнятся на destination с проверкой через `system.tables`.

### 6. Миграция данных

- Нажмите **"Мигрировать данные"** — для каждой выбранной таблицы данные будут прочитаны из source и записаны в destination.
- Прогресс и ошибки отображаются в логе внизу.

## Структура проекта

```
ch_copy/
├── ch_migrate.py      # Основное приложение
├── requirements.txt   # Python-зависимости
├── .env.example       # Шаблон конфигурации
├── .env               # Конфигурация (не в git)
├── .gitignore
└── venv/              # Виртуальное окружение (не в git)
```
