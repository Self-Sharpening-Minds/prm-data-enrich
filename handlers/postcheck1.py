import logging

import config
from llm.llm_client import LlmClient
from utils.db import AsyncDatabaseManager

logger = logging.getLogger(__name__)


async def fetch_person_for_postcheck1(db: AsyncDatabaseManager, person_id: int) -> dict | None:
    """
    Получает данные человека для PostCheck1.

    Args:
        db: Асинхронный менеджер базы данных.
        person_id: ID человека.

    Returns:
        dict | None: Данные человека или None, если человек не найден или Perplexity не выполнен.
    """
    query = f"""
        SELECT person_id, summary, urls, confidence
        FROM {config.result_table_name}
        WHERE person_id = $1 AND flag_perp = TRUE
    """
    rows = await db.fetch(query, person_id)
    if not rows:
        logger.warning(f"[person_id={person_id}] ⚠️ Человек не найден или Perplexity не выполнен")
        return None
    logger.debug(f"[person_id={person_id}] Данные для PostCheck1 успешно извлечены")
    return rows[0]


async def save_postcheck1_result(db: AsyncDatabaseManager, person_id: int, success: bool) -> None:
    """
    Сохраняет результат первой проверки summary через LLM.

    Args:
        db: Асинхронный менеджер базы данных.
        person_id: ID человека.
        success: True, если проверка успешна, False в случае ошибки.
    """
    query = f"UPDATE {config.result_table_name} SET flag_postcheck1 = $1"
    if not success:
        query += ", done = FALSE"
    query += " WHERE person_id = $2"

    await db.execute(query, success, person_id)
    logger.debug(f"[person_id={person_id}] Результат PostCheck1 сохранен: {success}")


async def perform_postcheck1(llm_client: LlmClient, summary: str, worker_id: int, person_id: int) -> bool:
    """
    Выполняет асинхронную проверку summary через LLM.

    Args:
        llm_client: Клиент LLM.
        summary: Текст summary для проверки.
        worker_id: ID воркера.
        person_id: ID человека.

    Returns:
        bool: True, если summary валиден, False — если нет.
    """
    logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] ▶️ Запуск проверки PostCheck1")
    return await llm_client.async_postcheck(summary)


async def run(worker_id: int, person_id: int) -> None:
    """
    Основной обработчик PostCheck1 для одного человека.

    Args:
        worker_id: ID воркера.
        person_id: ID человека.
    """
    logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] Начинаем PostCheck1")

    db = AsyncDatabaseManager()
    await db.connect()
    llm_client = LlmClient()

    try:
        person_data = await fetch_person_for_postcheck1(db, person_id)
        if not person_data:
            return

        summary = person_data.get("summary", "")
        is_valid = await perform_postcheck1(llm_client, summary, worker_id, person_id)

        await save_postcheck1_result(db, person_id, True)

        logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] ✅ PostCheck1 завершен. Валидность: {is_valid}")

    except Exception as e:
        await save_postcheck1_result(db, person_id, False)
        logger.exception(f"[Воркер #{worker_id}][person_id={person_id}] ❌ Ошибка в PostCheck1 handler: {e}")
        raise

    finally:
        await db.close()
