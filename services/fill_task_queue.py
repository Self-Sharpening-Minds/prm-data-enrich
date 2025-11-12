import logging

import config
from utils.db import AsyncDatabaseManager

logger = logging.getLogger(__name__)

TASK_TYPES = ["prellm", "llm", "perp", "postcheck1", "postcheck2", "photos"]

TASK_RULES = {
    "prellm": "TRUE",
    "llm": "flag_prellm = TRUE",
    "perp": "valid = TRUE",
    "postcheck1": "flag_perp = TRUE",
    "postcheck2": "flag_postcheck1 = TRUE",
    "photos": "done = TRUE"
}

TASK_FLAGS = {
    "prellm": "flag_prellm",
    "llm": "flag_llm",
    "perp": "flag_perp",
    "postcheck1": "flag_postcheck1",
    "postcheck2": "flag_postcheck2",
    "photos": "flag_photos"
}


async def fill_task_queue() -> None:
    db = AsyncDatabaseManager()
    await db.connect()
    try:
        for task_type in TASK_TYPES:
            condition = TASK_RULES[task_type]
            done_flag = TASK_FLAGS[task_type]

            logger.debug(f"Проверяем задачи типа '{task_type}'...")

            insert_query = f"""
                INSERT INTO task_queue (person_id, task_type, status)
                SELECT p.person_id, '{task_type}', 'pending'
                FROM {config.result_table_name} AS p
                WHERE {condition}
                  AND (p.{done_flag} IS NULL OR p.{done_flag} = FALSE)
            """
            # AND NOT EXISTS (
            #           SELECT 1 FROM task_queue tq
            #           WHERE tq.person_id = p.person_id
            #             AND tq.task_type = '{task_type}'
            #       );

            await db.execute(insert_query)
            # TODO: logger.info(f"Добавлены новые задачи типа '{task_type}': {количество}")

        logger.info("Очередь задач успешно обновлена.")

    finally:
        await db.close()
