# handlers/postcheck2.py
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
    """Вторая проверка summary через LLM с контекстом персоны."""
    logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] ▶️ Начинаем PostCheck2")

    db = AsyncDatabaseManager()
    await db.connect()
    llm_client = get_llm_client()

    try:
        # Получаем полные данные человека для второй проверки
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

        # Вторая проверка summary с контекстом персоны
        is_summary_valid = await llm_client.async_postcheck2(person_data, summary, urls)

        if is_summary_valid:
            # Если summary прошел обе проверки, оставляем как есть
            logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] ✅ Summary прошел обе проверки")
        else:
            # Если не прошел вторую проверку, очищаем результаты
            await db.execute(
                "UPDATE {config.result_table_name} SET summary = NULL, urls = NULL, confidence = 'second_check_failed' WHERE person_id = $1",
                person_id
            )

        # Помечаем как выполненную задачу postcheck2
        await db.execute(
            f"UPDATE {config.result_table_name} SET flag_postcheck2 = TRUE, done = TRUE WHERE person_id = $1",
            person_id
        )

        logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] ✅ PostCheck2 завершен. Валидность: {is_summary_valid}")

    except Exception as e:
        logger.exception(f"[Воркер #{worker_id}][person_id={person_id}] ❌ Ошибка в PostCheck2 handler: {e}")
        raise
    finally:
        await db.close()
