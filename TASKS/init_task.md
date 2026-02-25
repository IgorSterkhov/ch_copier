Создай полное рабочее Python-приложение с GUI на Tkinter для миграции таблиц из исходного ClickHouse (source) в целевой ClickHouse (destination). 
Используй библиотеку clickhouse-connect для подключений. Загружай конфигурацию из локального .env файла (используй python-dotenv): 
SOURCE_HOST, SOURCE_PORT (8123/8443), SOURCE_USER, SOURCE_PASS, SOURCE_DB, SOURCE_SECURE (True/False), SOURCE_CA_CERT (путь к .crt для SSL на 8443); 
аналогично для DESTINATION_*, где destination порт может быть 8123, 9000 или 8443 с SSL.

Структура GUI (используй Treeview для дерева схемы
- Левая панель: дерево схемы source (база -> таблицы/вью). При клике на таблицу показывай DDL (SHOW CREATE TABLE) в отдельной Text области.
- Кнопка "Подключиться" для обоих CH при запуске.
- Чекбоксы для выбора таблиц из разных баз.
- Для выбранных таблиц генерируй редактируемые SQL:
  - SELECT * FROM table WHERE date_field >= 'YYYY-MM-DD' (выбор поля даты из колонок, опционально) AND LIMIT N (опционально, или без).
  - Поддержка Dictionary таблиц (SELECT для словарей).
  - DDL скрипты (CREATE TABLE из SHOW CREATE).

Логика миграции:
- Кнопка "Генерировать DDL": очисти ENGINE для Replicated* (regex: удали всё в скобках после ENGINE=Replicated.*, оставь ENGINE=ReplicatedMergeTree ORDER BY ...).
- Кнопка "Создать DDL на destination": CREATE DATABASE IF NOT EXISTS, затем CREATE OR REPLACE TABLE из отредактированных DDL. Покажи статус/лог.
- Кнопка "Мигрировать данные": для каждой выбранной таблицы INSERT INTO dest.table SELECT ... FROM source.table (с лимитами WHERE/LIMIT). Покажи прогресс/ошибки.
- Проверяй создание DDL (query system.tables).

Код должен быть полным, с импортами (tkinter, ttk, clickhouse_connect, dotenv), обработкой ошибок, скроллами, копированием в буфер. Сохраняй состояние подключений. Тестируй на примерах DDL ReplicatedMergeTree.

