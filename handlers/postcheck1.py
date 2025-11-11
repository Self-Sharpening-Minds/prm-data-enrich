# handlers/postcheck1.py
import logging
from utils.db import AsyncDatabaseManager
from llm.llm_client import LlmClient
import config

logger = logging.getLogger(__name__)

# Глобальный клиент LLM для переиспользования
_llm_client = None

def get_llm_client():
    """Получает или создает глобальный клиент LLM."""
    global _llm_client
    if _llm_client is None:
        _llm_client = LlmClient()
    return _llm_client


async def run(worker_id: int, person_id: int):
    """Первая проверка summary через LLM."""
    logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] ▶️ Начинаем PostCheck1")

    db = AsyncDatabaseManager()
    await db.connect()
    llm_client = get_llm_client()

    try:
        # Получаем данные человека с результатами Perplexity
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

        # Первая проверка summary
        is_summary_valid = await llm_client.async_postcheck(summary)

        if not is_summary_valid:
            # Если summary не прошел проверку, обновляем confidence
            await db.execute(
                "UPDATE {config.result_table_name} SET confidence = 'first_check_failed' WHERE person_id = $1",
                person_id
            )

        # Помечаем как выполненную задачу postcheck1
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
        