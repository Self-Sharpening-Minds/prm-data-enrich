import logging
from typing import Any

import asyncpg
from config import DatabaseConfig


class DatabaseManager:
    """Менеджер асинхронной для работы с базой данных PostgreSQL.
    Обеспечивает подключение к БД, выполнение запросов, создание таблиц
    и другие операции с базой данных.
    Attributes:
        config (DatabaseConfig): Конфигурация подключения к БД
        connection: Соединение с базой данных
        logger: Логгер для записи событий
    """

    def __init__(self, config: DatabaseConfig | None = None) -> None:
        self.config = config or DatabaseConfig()
        self.pool: asyncpg.Pool | None = None
        self.logger = logging.getLogger(__name__)

    async def connect(self):
        """Создание пула соединений с базой данных."""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                min_size=1,
                max_size=10,
            )
            self.logger.info(
                f"Успешное подключение к БД: {self.config.host}:{self.config.port}/{self.config.database}"
            )
        except Exception as e:
            self.logger.error(f"Ошибка подключения к БД: {e}")
            raise

    async def close(self):
        """Закрытие пула соединений."""
        if self.pool:
            await self.pool.close()
            self.logger.info("Соединение с БД закрыто")

    async def execute(self, query: str, *params) -> str:
        """Выполнение запроса без возврата результата (INSERT/UPDATE/DELETE)."""
        if not self.pool:
            raise RuntimeError("Нет активного подключения к БД")
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(query, *params)
        return "ok"

    async def fetch(self, query: str, *params) -> list[dict[str, Any]]:
        """Выполнение SELECT-запроса."""
        if not self.pool:
            raise RuntimeError("Нет активного подключения к БД")
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [dict(r) for r in rows]

    async def fetchrow(self, query: str, *params) -> dict[str, Any] | None:
        """Получение одной строки."""
        if not self.pool:
            raise RuntimeError("Нет активного подключения к БД")
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, *params)
            return dict(row) if row else None

    async def create_cleaned_table(self, source_table_name: str, new_table_name: str) -> bool:
        """Создание таблицы с очищенными данными."""
        query = f"""
            DROP TABLE IF EXISTS {new_table_name};
            CREATE TABLE {new_table_name} AS
            SELECT DISTINCT ON ((data->>'telegram_id')::bigint) *
            FROM {source_table_name}
            WHERE data ? 'about'
            ORDER BY (data->>'telegram_id')::bigint, fetch_date DESC;
        """
        try:
            await self.execute(query)
            self.logger.info(f"✅ Таблица {new_table_name} создана успешно")
            return True
        except Exception as e:
            self.logger.error(f"Ошибка при создании таблицы {new_table_name}: {e}")
            return False

    async def create_result_table(self, source_table_name: str,
                                  result_table_name: str, drop_table: bool = False) -> bool:
        """Создание результирующей таблицы."""
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    if drop_table:
                        await conn.execute(f"DROP TABLE IF EXISTS {result_table_name};")

                    query = f"""
                        CREATE TABLE {result_table_name} AS
                        SELECT
                            person_id::bigint AS person_id,
                            fetch_date::timestamp without time zone AS fetch_date,
                            (data->>'telegram_id')::bigint AS telegram_id,
                            data->>'first_name' AS first_name,
                            data->>'last_name' AS last_name,
                            data->>'birth_date' as birth_date,
                            data->>'about' AS about,
                            data->>'username' AS username,
                            data->'personal_channel'->>'title' AS personal_channel_title,
                            data->'personal_channel'->>'username' AS personal_channel_username,
                            data->'personal_channel'->>'about' AS personal_channel_about,
                            (data->'personal_channel'->>'channel_id')::bigint AS personal_channel_id,
                            false AS flag_prellm,
                            false AS flag_llm,
                            false AS valid,
                            false AS flag_perp,
                            false AS flag_postcheck1,
                            false AS flag_postcheck2,
                            false AS done,
                            false AS flag_photos,
                            null::text AS meaningful_first_name,
                            null::text AS meaningful_last_name,
                            null::text AS meaningful_about,
                            ARRAY[]::text[] AS extracted_links,
                            null::text AS summary,
                            null::text AS confidence,
                            ARRAY[]::text[] AS urls,
                            ARRAY[]::text[] AS photos
                        FROM {source_table_name};
                    """
                    await conn.execute(query)
                    self.logger.info(f"✅ Таблица {result_table_name} успешно создана")
            return True
        except Exception as e:
            self.logger.error(f"Ошибка при создании {result_table_name}: {e}")
            return False

    async def test_connection(self) -> bool:
        """Тест подключения к базе."""
        try:
            result = await self.fetchrow("SELECT version(), current_database(), current_user;")
            if result:
                self.logger.info(
                    f"🔌 Подключение к БД успешно: {result['current_database']} ({result['version']})"
                )
                return True
            return False
        except Exception as e:
            self.logger.error(f"Ошибка при тестировании подключения: {e}")
            return False

    async def get_table_info(self, table_name: str) -> list[dict[str, Any]]:
        """Получение структуры таблицы."""
        query = """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = $1
            ORDER BY ordinal_position
        """
        return await self.fetch(query, table_name)
