import logging

import config
from llm.perp_client import PerplexityClient
from utils.db import AsyncDatabaseManager

logger = logging.getLogger(__name__)


async def fetch_person_data(db: AsyncDatabaseManager, person_id: int) -> dict | None:
    """
    Получает валидные данные человека из базы для поиска Perplexity.

    Args:
        db: Асинхронный менеджер базы данных.
        person_id: ID человека.

    Returns:
        dict | None: Данные человека или None, если человек не найден/невалиден.
    """
    query = f"""
        SELECT person_id, meaningful_first_name, meaningful_last_name,
               meaningful_about, extracted_links
        FROM {config.result_table_name}
        WHERE person_id = $1 AND valid = TRUE
    """
    rows = await db.fetch(query, person_id)
    if not rows:
        logger.warning(f"[person_id={person_id}] ⚠️ Человек не найден или невалиден")
        return None
    logger.debug(f"[person_id={person_id}] Данные успешно извлечены из БД")
    return rows[0]


async def save_perplexity_result(db: AsyncDatabaseManager, person_id: int, summary: str, urls: list, confidence: str) -> None:
    """
    Сохраняет результаты поиска Perplexity в базу данных.

    Args:
        db: Менеджер базы данных.
        person_id: ID человека.
        summary: Итоговое summary.
        urls: Список найденных ссылок.
        confidence: Уровень доверия ('low', 'medium', 'high').
    """
    await db.execute(config.UPDATE_SUMMARY_QUERY, summary, urls, confidence, person_id)
    await db.execute(f"UPDATE {config.result_table_name} SET flag_perp = TRUE WHERE person_id = $1", person_id)
    logger.debug(f"[person_id={person_id}] Результаты Perplexity успешно сохранены")


async def perform_perplexity_search(perp_client: PerplexityClient, person_data: dict, worker_id: int) -> dict:
    """
    Выполняет асинхронный поиск информации через Perplexity для одного человека.

    Args:
        perp_client: Клиент Perplexity.
        person_data: Данные человека.
        worker_id: ID воркера для логирования.

    Returns:
        dict: Результат поиска с ключами 'summary', 'urls', 'confidence'.
    """
    logger.debug(f"[Воркер #{worker_id}][person_id={person_data['person_id']}] Запуск Perplexity поиска")
    return await perp_client.async_search_info(person_data=person_data)


async def run(worker_id: int, person_id: int) -> bool:
    """
    Основной обработчик Perplexity для одного человека.

    Args:
        worker_id: ID воркера.
        person_id: ID человека.
    """
    db = AsyncDatabaseManager()
    await db.connect()
    perp_client = PerplexityClient()

    try:
        person_data = await fetch_person_data(db, person_id)
        if not person_data:
            return False

        search_result = await perform_perplexity_search(perp_client, person_data, worker_id)

        summary = search_result.get("summary", "")
        urls = search_result.get("urls", [])
        confidence = search_result.get("confidence", "заглушка")  # TODO: система confidence

        await save_perplexity_result(db, person_id, summary, urls, confidence)
        logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] ✅ Perplexity поиск завершен успешно")
        return True

    except Exception as e:
        await db.execute(f"UPDATE {config.result_table_name} SET flag_perp = FALSE WHERE person_id = $1", person_id)
        logger.exception(f"[Воркер #{worker_id}][person_id={person_id}] ❌ Ошибка в Perplexity handler: {e}")
        raise

    finally:
        await db.close()
