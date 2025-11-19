import logging

import config
from utils.db import AsyncDatabaseManager

logger = logging.getLogger(__name__)


class TaskQueue:
    """
    Класс для управления очередью задач.
    """

    def __init__(self) -> None:
        """Создаёт подключение к базе данных."""
        self.db = AsyncDatabaseManager()

    async def connect(self) -> None:
        """Открывает соединение с базой."""
        await self.db.connect()

    async def close(self) -> None:
        """Закрывает соединение с базой."""
        await self.db.close()

    async def _insert_task(self, person_id: int, task_type: str) -> None:
        """
        Добавляет задачу в очередь по персоне

        Args:
            person_id: ID персоны
            task_type: тип задачи
        """
        condition = config.TASK_RULES[task_type]

        query = f"""
            INSERT INTO task_queue (person_id, task_type, status)
            SELECT {person_id}, '{task_type}', 'pending'
            FROM {config.result_table_name} AS p
            WHERE p.person_id = {person_id}
                AND {condition}
                AND
                    NOT EXISTS (
                        SELECT 1 FROM task_queue
                        WHERE person_id = {person_id} AND task_type = '{task_type}'
                    )
        """

        await self.db.execute(query)

    async def _insert_tasks_bulk(self, task_type: str) -> None:
        """
        Добавляет задачи указанного типа для всех подходящих персон.

        Args:
            task_type: тип задачи
        """
        condition = config.TASK_RULES[task_type]
        done_flag = config.TASK_FLAGS[task_type]

        logger.debug(f"Bulk insert задач типа '{task_type}' с условием: {condition}")

        count_query = f"""
            SELECT COUNT(*) as count
            FROM {config.result_table_name} AS p
            WHERE {condition}
              AND (p.{done_flag} IS NULL OR p.{done_flag} = FALSE)
              AND 
                NOT EXISTS (
                    SELECT 1 FROM task_queue tq
                    WHERE tq.person_id = p.person_id
                      AND tq.task_type = '{task_type}'
                )
        """ #TODO: сделать нормальный подсчет

        count_select_result = await self.db.fetch(count_query)
        expected_count = count_select_result[0]['count'] if count_select_result else 0

        if expected_count > 0:
            insert_query = f"""
                INSERT INTO task_queue (person_id, task_type, status)
                SELECT p.person_id, '{task_type}', 'pending'
                FROM {config.result_table_name} AS p
                WHERE {condition}
                AND (p.{done_flag} IS NULL OR p.{done_flag} = FALSE)
                AND NOT EXISTS (
                        SELECT 1 FROM task_queue tq
                        WHERE tq.person_id = p.person_id
                        AND tq.task_type = '{task_type}'
                )
            """
            await self.db.execute(insert_query)
        
        logger.debug(f"Добавление задач типа '{task_type}' завершено. Добавлено задач: {expected_count}")

    async def fill_all(self) -> None:
        """
        Обновляет всю очередь задач:
        для каждого task_type добавляет задачи, если они ещё не созданы.
        """
        await self.connect()

        try:
            logger.info("Обновляем всю очередь задач...")

            for task_type in config.TASK_TYPES:
                logger.debug(f"Обрабатываем задания типа '{task_type}'...")
                await self._insert_tasks_bulk(task_type)

            logger.info("Вся очередь задач успешно обновлена.")
        finally:
            await self.close()

    async def add_for_person(self, person_id: int, task_type: str) -> None:
        """
        Добавляет задачу конкретному человеку, если её нет.

        Args:
            person_id: ID персоны
            task_type: тип задачи (prellm, llm, perp и т.д.)
        """
        if task_type not in config.TASK_TYPES:
            raise ValueError(f"Неизвестный тип задачи: {task_type}")

        await self.connect()
        try:
            await self._insert_task(person_id, task_type)
            logger.debug(f"[person_id={person_id}] Добавлена задача '{task_type}'.")
        finally:
            await self.close()
