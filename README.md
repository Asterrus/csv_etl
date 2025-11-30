### Описание

Загрузка данных из CSV-файлов в базу данных PostgreSQL с преобразованием данных с помощью Pandas.

### Инструменты

- docker
- docker-compose
- uv

### Запуск

1. Клонировать репозиторий и перейти в директорию проекта:

   ```bash
   git clone ...
   cd csv_etl
   ```

2. Создать .env файл с переменными окружения:

   ```bash
   cp .env.example .env
   ```

3. Запустить docker-compose для запуска базы данных:

   ```bash
   docker-compose up -d
   ```

4. Запустить ETL-процесс:

   ```bash
   uv run main.py
   ```
