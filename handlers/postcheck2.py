import logging

import config
from llm.llm_client import LlmClient
from utils.db import AsyncDatabaseManager

logger = logging.getLogger(__name__)


async def fetch_person_for_postcheck2(db: AsyncDatabaseManager, person_id: int) -> dict | None:
    """
    Получает данные человека для PostCheck2.

    Args:
        db: Асинхронный менеджер базы данных.
        person_id: ID человека.

    Returns:
        dict | None: Данные человека или None, если человек не найден
                     или PostCheck1 не выполнен.
    """
    query = f"""
        SELECT person_id, meaningful_first_name, meaningful_last_name,
               meaningful_about, extracted_links, summary, urls, confidence
        FROM {config.result_table_name}
        WHERE person_id = $1 AND flag_postcheck1 = TRUE
    """
    rows = await db.fetch(query, person_id)
    if not rows:
        logger.warning(f"[person_id={person_id}] ⚠️ Человек не найден или PostCheck1 не выполнен")
        return None
    logger.debug(f"[person_id={person_id}] Данные для PostCheck2 успешно извлечены")
    return rows[0]


async def save_postcheck2_result(db: AsyncDatabaseManager, person_id: int, success: bool) -> None:
    """
    Сохраняет результат второй проверки summary через LLM.

    Args:
        db: Асинхронный менеджер базы данных.
        person_id: ID человека.
        success: True, если проверка успешна, False — если нет.
    """
    query = f"UPDATE {config.result_table_name} SET flag_postcheck2 = $1"
    if success:
        query += ", done = TRUE"
    else:
        query += ", done = FALSE"
    query += " WHERE person_id = $2"

    await db.execute(query, success, person_id)
    logger.debug(f"[person_id={person_id}] Результат PostCheck2 сохранен: {success}")


async def perform_postcheck2(llm_client: LlmClient, person_data: dict, summary: str, urls: list, worker_id: int, person_id: int) -> bool:
    """
    Выполняет асинхронную проверку summary через LLM с контекстом персоны.

    Args:
        llm_client: Клиент LLM.
        person_data: Данные человека из БД.
        summary: Текст summary для проверки.
        urls: Список URL, связанных с персоной.
        worker_id: ID воркера.
        person_id: ID человека.

    Returns:
        bool: True, если summary валиден, False в противном случае.
    """
    logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] Запуск проверки PostCheck2")
    return await llm_client.async_postcheck2(person_data, summary, urls)


async def run(worker_id: int, person_id: int) -> bool:
    """
    Основной обработчик PostCheck2 для одного человека.

    Args:
        worker_id: ID воркера.
        person_id: ID человека.
    """
    logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] Начинаем PostCheck2")

    db = AsyncDatabaseManager()
    await db.connect()
    llm_client = LlmClient()

    try:
        person_data = await fetch_person_for_postcheck2(db, person_id)
        if not person_data:
            return False

        summary = person_data.get("summary", "")
        urls = person_data.get("urls", [])

        is_valid = await perform_postcheck2(llm_client, person_data, summary, urls, worker_id, person_id)

        await save_postcheck2_result(db, person_id, is_valid)
        logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] ✅ PostCheck2 завершен. Валидность: {is_valid}")
        return is_valid

    except Exception as e:
        await save_postcheck2_result(db, person_id, False)
        logger.exception(f"[Воркер #{worker_id}][person_id={person_id}] ❌ Ошибка в PostCheck2 handler: {e}")
        raise

    finally:
        await db.close()
