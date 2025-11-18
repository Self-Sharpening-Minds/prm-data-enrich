import logging

import config
from llm.llm_client import LlmClient
from utils.db import AsyncDatabaseManager

logger = logging.getLogger(__name__)


async def fetch_person_data(db: AsyncDatabaseManager, person_id: int) -> dict | None:
    """
    Получает подготовленные данные человека для LLM из базы данных.

    Args:
        db: Асинхронный менеджер базы данных.
        person_id: ID персоны.

    Returns:
        dict | None: Словарь с данными или None, если человек не найден.
    """
    query = f"""
        SELECT person_id, meaningful_first_name, meaningful_last_name, meaningful_about, extracted_links
        FROM {config.result_table_name}
        WHERE person_id = $1
    """
    rows = await db.fetch(query, person_id)
    if not rows:
        logger.warning(f"[person_id={person_id}] ⚠️ Человек не найден в БД")
        return None
    return rows[0]


async def attempt_llm_parse(
    llm: LlmClient,
    llm_input: dict,
    db: AsyncDatabaseManager,
    person_id: int,
    worker_id: int
) -> bool:
    """
    Пытается обработать одного человека через LLM с несколькими попытками.

    Args:
        llm: Клиент LLM.
        llm_input: Входные данные для LLM.
        db: Менеджер базы данных.
        person_id: ID человека.
        worker_id: ID воркера для логирования.

    Returns:
        bool: True, если обработка успешна, False иначе.
    """
    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            result = await llm.async_parse_single_to_meaningful(llm_input)
            if not isinstance(result, dict) or not result:
                logger.warning(
                    f"[Воркер #{worker_id}][person_id={person_id}] ❌ "
                    f"LLM вернула некорректный результат. Попытка {attempt}/{config.MAX_RETRIES}. "
                    f"Тип: {type(result)}"
                )
                continue

            first_name = result.get('meaningful_first_name')
            last_name = result.get('meaningful_last_name')
            about = result.get('meaningful_about')
            extracted_links = llm_input.get('extracted_links')

            is_valid = bool(first_name and last_name and (about or extracted_links))

            await db.execute(
                config.UPDATE_LLM_RESULTS_QUERY,
                first_name, last_name, about, is_valid, person_id
            )

            logger.debug(
                f"[Воркер #{worker_id}][person_id={person_id}] "
                f"LLM обработка успешна на попытке {attempt}"
            )
            return True

        except Exception as e:
            logger.error(
                f"[Воркер #{worker_id}][person_id={person_id}] "
                f"Ошибка при обработке LLM на попытке {attempt}: {e}",
                exc_info=True
            )

    logger.error(f"[Воркер #{worker_id}][person_id={person_id}] ❌ Все {config.MAX_RETRIES} попыток LLM неудачны")
    return False


async def process_single_person(
    llm: LlmClient,
    db: AsyncDatabaseManager,
    person_id: int,
    worker_id: int
) -> bool:
    """
    Обрабатывает одного человека через LLM.

    Args:
        llm: Клиент LLM.
        db: Менеджер базы данных.
        person_id: ID человека.
        worker_id: ID воркера.

    Returns:
        bool: True, если успешно, False иначе.
    """
    logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] Начало LLM обработки")

    person_data = await fetch_person_data(db, person_id)
    if not person_data:
        return False

    llm_input = {
        "person_id": person_data["person_id"],
        "first_name": person_data.get("meaningful_first_name", ""),
        "last_name": person_data.get("meaningful_last_name", ""),
        "about": person_data.get("meaningful_about", ""),
        "extracted_links": person_data.get("extracted_links")
    }

    return await attempt_llm_parse(llm, llm_input, db, person_id, worker_id)


async def run(worker_id: int, person_id: int) -> bool:
    """
    Основной обработчик LLM для одного человека.

    Args:
        worker_id: ID воркера.
        person_id: ID человека.
    """
    logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] Запуск LLM handler")

    db = AsyncDatabaseManager()
    await db.connect()
    llm = LlmClient()

    try:
        success = await process_single_person(llm, db, person_id, worker_id)
        flag_query = f"UPDATE {config.result_table_name} SET flag_llm = $1 WHERE person_id = $2"

        if success:
            await db.execute(flag_query, True, person_id)
            logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] ✅ LLM завершен успешно")
            return True
        else:
            await db.execute(flag_query, False, person_id)
            logger.error(f"[Воркер #{worker_id}][person_id={person_id}] ❌ LLM завершен с ошибкой")
            raise Exception(f"Не удалось обработать person_id {person_id} через LLM после {config.MAX_RETRIES} попыток")

    except Exception as e:
        await db.execute(flag_query, False, person_id)
        logger.exception(f"[Воркер #{worker_id}][person_id={person_id}] ❌ Ошибка в LLM handler: {e}")
        raise

    finally:
        await db.close()
