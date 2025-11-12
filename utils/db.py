import logging
from typing import Any

import asyncpg
from config import DatabaseConfig


class AsyncDatabaseManager:
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
            self.logger.debug(
                f"Успешное подключение к БД: {self.config.host}:{self.config.port}/{self.config.database}"
            )
        except Exception as e:
            self.logger.error(f"Ошибка подключения к БД: {e}")
            raise

    async def close(self):
        """Закрытие пула соединений."""
        if self.pool:
            await self.pool.close()
            self.logger.debug("Соединение с БД закрыто")

    async def execute(self, query: str, *params) -> str:
        """Выполнение запроса без возврата результата."""
        if not self.pool:
            raise RuntimeError("Нет активного подключения к БД")
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(query, *params)
        return "ok"

    async def fetch(self, query: str, *params) -> list[dict[str, Any]]:
        """Выполнение запроса с возвратом результата."""
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
