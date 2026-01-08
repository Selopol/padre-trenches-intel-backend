# Railway PostgreSQL Setup Guide

## Проблема
SQLite база данных сбрасывается при каждом перезапуске сервиса на Railway, так как файловая система эфемерная.

## Решение
Использовать PostgreSQL базу данных Railway, которая сохраняет данные постоянно.

## Шаги настройки

### 1. Добавить PostgreSQL в Railway проект

1. Откройте проект в Railway dashboard
2. Нажмите **"Create"** → **"Database"** → **"Add PostgreSQL"**
3. Railway автоматически создаст базу данных и добавит переменную окружения `DATABASE_URL`

### 2. Переменные окружения

Railway автоматически добавит переменную:
```
DATABASE_URL=postgresql://user:password@host:port/database
```

Код автоматически определит наличие `DATABASE_URL` и переключится на PostgreSQL.

### 3. Проверка

После деплоя проверьте логи:
```
Database initialized successfully (PostgreSQL)
```

Вместо:
```
Database initialized successfully (SQLite)
```

## Как это работает

### Автоопределение базы данных

```python
DATABASE_URL = os.getenv("DATABASE_URL", None)  # PostgreSQL URL from Railway
DATABASE_PATH = os.getenv("DATABASE_PATH", "dev_intel.db")  # SQLite fallback
USE_POSTGRES = DATABASE_URL is not None and POSTGRES_AVAILABLE
```

### Универсальное подключение

```python
def get_connection():
    if USE_POSTGRES:
        return psycopg2.connect(DATABASE_URL)
    else:
        return sqlite3.connect(DATABASE_PATH)
```

### Схема базы данных

Код автоматически создаёт правильную схему для каждой БД:
- **PostgreSQL**: `SERIAL PRIMARY KEY`, `BOOLEAN DEFAULT FALSE`
- **SQLite**: `INTEGER PRIMARY KEY AUTOINCREMENT`, `BOOLEAN DEFAULT 0`

## Преимущества

✅ **Постоянное хранение** - данные не теряются при перезапуске  
✅ **Автоматическое переключение** - работает локально (SQLite) и в production (PostgreSQL)  
✅ **Без изменений кода** - просто добавьте PostgreSQL в Railway  
✅ **Масштабируемость** - PostgreSQL лучше подходит для production  

## Локальная разработка

Для локальной разработки код автоматически использует SQLite:
```bash
python3 server.py
# Database initialized successfully (SQLite)
```

## Production

На Railway с PostgreSQL:
```bash
# DATABASE_URL установлен автоматически
# Database initialized successfully (PostgreSQL)
```

## Миграция данных (опционально)

Если нужно перенести существующие данные из SQLite в PostgreSQL:

1. Экспортируйте данные из SQLite
2. Импортируйте в PostgreSQL через Railway CLI или pgAdmin

Но обычно это не нужно, так как сканер автоматически заполнит базу новыми данными.
