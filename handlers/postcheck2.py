import logging

import config
from llm.llm_client import LlmClient
from utils.db import AsyncDatabaseManager

logger = logging.getLogger(__name__)


async def run(worker_id: int, person_id: int):
    """Вторая проверка summary через LLM с контекстом персоны."""
    logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] Начинаем PostCheck2")

    db = AsyncDatabaseManager()
    await db.connect()
    llm_client = LlmClient()

    try:
        select_query = f"""
            SELECT person_id, meaningful_first_name, meaningful_last_name,
                   meaningful_about, extracted_links, summary, urls, confidence
            FROM {config.result_table_name}
            WHERE person_id = $1 AND flag_postcheck1 = TRUE
        """
        person_rows = await db.fetch(select_query, person_id)

        if not person_rows:
            logger.warning(f"[Воркер #{worker_id}][person_id={person_id}] ⚠️ Человек не найден или PostCheck1 не выполнен")
            return

        person_data = person_rows[0]
        summary = person_data.get("summary", "")
        urls = person_data.get("urls", [])

        is_summary_valid = await llm_client.async_postcheck2(person_data, summary, urls)

        await db.execute(
            f"UPDATE {config.result_table_name} SET flag_postcheck2 = TRUE, done = TRUE WHERE person_id = $1",
            person_id
        )

        logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] ✅ PostCheck2 завершен. Валидность: {is_summary_valid}")

    except Exception as e:
        await db.execute(
            f"UPDATE {config.result_table_name} SET flag_postcheck2 = FALSE, done = FALSE WHERE person_id = $1",
            person_id
        )
        logger.exception(f"[Воркер #{worker_id}][person_id={person_id}] ❌ Ошибка в PostCheck2 handler: {e}")
        raise
    finally:
        await db.close()
