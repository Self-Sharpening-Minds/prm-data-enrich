# handlers/postcheck1.py
import logging

import config
from llm.llm_client import LlmClient
from utils.db import AsyncDatabaseManager

logger = logging.getLogger(__name__)


async def run(worker_id: int, person_id: int):
    """Первая проверка summary через LLM."""
    logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] Начинаем PostCheck1")

    db = AsyncDatabaseManager()
    await db.connect()
    llm_client = LlmClient()

    try:
        select_query = f"""
            SELECT person_id, summary, urls, confidence
            FROM {config.result_table_name}
            WHERE person_id = $1 AND flag_perp = TRUE
        """
        person_rows = await db.fetch(select_query, person_id)

        if not person_rows:
            logger.warning(f"[Воркер #{worker_id}][person_id={person_id}] ⚠️ Человек не найден или Perplexity не выполнен")
            return

        person_data = person_rows[0]
        summary = person_data.get("summary", "")

        is_summary_valid = await llm_client.async_postcheck(summary)

        await db.execute(
            f"UPDATE {config.result_table_name} SET flag_postcheck1 = TRUE WHERE person_id = $1",
            person_id
        )

        logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] ✅ PostCheck1 завершен. Валидность: {is_summary_valid}")

    except Exception as e:
        logger.exception(f"[Воркер #{worker_id}][person_id={person_id}] ❌ Ошибка в PostCheck1 handler: {e}")
        raise
    finally:
        await db.close()
