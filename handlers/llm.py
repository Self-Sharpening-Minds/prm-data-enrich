import asyncio
import logging

import config
from llm.llm_client import LlmClient
from utils.db import AsyncDatabaseManager

logger = logging.getLogger(__name__)


async def process_single_person(llm: LlmClient, db: AsyncDatabaseManager, person_id: int, worker_id: int) -> bool:
    """
    Обрабатывает одного человека через LLM с логикой повторных попыток.
    Args:
        llm: Клиент LLM
        db: Менеджер базы данных
        person_id: ID человека для обработки
        worker_id: ID воркера для логирования
    Returns:
        True в случае успеха, False в случае неудачи
    """
    logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] Обработка LLM.")

    try:
        select_query = f"""
            SELECT person_id, meaningful_first_name, meaningful_last_name, meaningful_about, extracted_links
            FROM {config.result_table_name}
            WHERE person_id = $1
        """
        person_rows = await db.fetch(select_query, person_id)

        if not person_rows:
            logger.warning(f"[Воркер #{worker_id}][person_id={person_id}] ⚠️ Человек не найден в БД")
            return False
        person_data = person_rows[0]

        llm_input = {
            "person_id": person_data["person_id"],
            "first_name": person_data.get("meaningful_first_name", ""),
            "last_name": person_data.get("meaningful_last_name", ""),
            "about": person_data.get("meaningful_about", "")
        }

        for attempt in range(config.MAX_RETRIES):
            parsed_result = await llm.async_parse_single_to_meaningful(llm_input)

            if not isinstance(parsed_result, dict) or not parsed_result:
                logger.warning(
                    f"[Воркер #{worker_id}][person_id={person_id}] ❌ LLM вернула некорректный результат."
                    f"Попытка {attempt + 1}/{config.MAX_RETRIES}. Тип: {type(parsed_result)}"
                )
                await asyncio.sleep(0.5)
                continue

            first_name = parsed_result.get('meaningful_first_name')
            last_name = parsed_result.get('meaningful_last_name')
            about = parsed_result.get('meaningful_about')
            extracted_links = person_data.get('extracted_links')
            is_valid = bool(first_name and last_name and (about or extracted_links))

            await db.execute(
                config.UPDATE_LLM_RESULTS_QUERY,
                first_name, last_name, about, is_valid, person_id
            )

            logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] LLM обработка завершена успешно")
            return True

    except Exception as e:
        logger.error(
            f"[Воркер #{worker_id}][person_id={person_id}] ❌ Ошибка при обработке LLM на попытке {attempt + 1}: {e}",
            exc_info=True
        )
        await asyncio.sleep(0.5)

    logger.error(f"[Воркер #{worker_id}][person_id={person_id}] ❌ Не удалось обработать после {config.MAX_RETRIES} попыток")
    return False


async def run(worker_id: int, person_id: int) -> None:
    """Асинхронная обработка одного человека через LLM."""
    logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] Начинаем LLM обработку")

    db = AsyncDatabaseManager()
    await db.connect()
    llm = LlmClient()

    try:
        success = await process_single_person(llm, db, person_id, worker_id)

        if success:
            await db.execute(
                f"UPDATE {config.result_table_name} SET flag_llm = TRUE WHERE person_id = $1",
                person_id
            )
            logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] ✅ LLM завершен успешно")
        else:
            logger.error(f"[Воркер #{worker_id}][person_id={person_id}] ❌ LLM завершен с ошибкой")
            raise Exception(f"Не удалось обработать person_id {person_id} через LLM после {config.MAX_RETRIES} попыток")

    except Exception as e:
        logger.exception(f"[Воркер #{worker_id}][person_id={person_id}] ❌ Ошибка в LLM handler: {e}")
        raise
    finally:
        await db.close()
