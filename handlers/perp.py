# handlers/perp.py
import logging
from utils.db import AsyncDatabaseManager
from llm.perp_client import PerplexityClient
import config

logger = logging.getLogger(__name__)

_perp_client = None

def get_perp_client():
    """Получает или создает глобальный клиент Perplexity."""
    global _perp_client
    if _perp_client is None:
        _perp_client = PerplexityClient()
    return _perp_client


async def run(worker_id: int, person_id: int):
    """Асинхронный поиск информации через Perplexity для одного человека."""
    logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] ▶️ Начинаем Perplexity поиск")

    db = AsyncDatabaseManager()
    await db.connect()
    perp_client = get_perp_client()

    try:
        select_query = f"""
            SELECT person_id, meaningful_first_name, meaningful_last_name, 
                   meaningful_about, extracted_links
            FROM {config.result_table_name} 
            WHERE person_id = $1 AND valid = TRUE
        """
        person_rows = await db.fetch(select_query, person_id)
        
        if not person_rows:
            logger.warning(f"[Воркер #{worker_id}][person_id={person_id}] ⚠️ Человек не найден или не валиден")
            return

        person_data = person_rows[0]

        search_result = await perp_client.async_search_info(
            first_name=person_data.get("meaningful_first_name", ""),
            last_name=person_data.get("meaningful_last_name", ""),
            about=person_data.get("meaningful_about", ""),
            extracted_links=person_data.get("extracted_links", [])
        )

        summary = search_result.get("summary", "")
        urls = search_result.get("urls", [])
        confidence = search_result.get("confidence", "low")

        await db.execute(
            config.UPDATE_SUMMARY_QUERY,
            summary,
            urls,
            confidence,
            person_id
        )

        await db.execute(
            f"UPDATE {config.result_table_name} SET flag_perp = TRUE WHERE person_id = $1",
            person_id
        )

        logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] ✅ Perplexity поиск завершен")

    except Exception as e:
        logger.exception(f"[Воркер #{worker_id}][person_id={person_id}] ❌ Ошибка в Perplexity handler: {e}")
        raise
    finally:
        await db.close()
