import logging

import config
from utils import cleaner
from utils.db import AsyncDatabaseManager

logger = logging.getLogger(__name__)


async def run(worker_id: int, person_id: int) -> None:
    """Асинхронная предварительная очистка данных перед обработкой LLM."""
    logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] Начинаем prellm")

    db = AsyncDatabaseManager()
    await db.connect()

    try:
        select_query = f"""
            SELECT person_id, first_name, last_name, about,
                   personal_channel_title, personal_channel_about
            FROM {config.result_table_name}
            WHERE person_id = $1
        """
        person_rows = await db.fetch(select_query, person_id)

        if not person_rows:
            logger.warning(f"[Воркер #{worker_id}][person_id={person_id}] ⚠️ Не найден в БД")
            return
        person = person_rows[0]

        fields_to_normalize = [
            'person_id', 'first_name', 'last_name',
            'about', 'personal_channel_title', 'personal_channel_about'
        ]
        for field in fields_to_normalize:
            person[field] = cleaner.normalize_empty(person.get(field))

        first_name = cleaner.clean_name_field(person.get('first_name'))
        last_name = cleaner.clean_second_name_field(person.get('last_name'))
        about = person.get('about')
        channel_title = person.get('personal_channel_title')
        channel_about = person.get('personal_channel_about')

        person_extracted_links = cleaner.extract_links(last_name, about, channel_about)

        if first_name and ' ' in first_name and not last_name:
            parts = first_name.split(' ', 1)
            if len(parts) == 2:
                first_name, last_name = parts

        about_clean = cleaner.merge_about_fields(about, channel_title, channel_about)

        params = (first_name, last_name, about_clean, person_extracted_links, person_id)
        await db.execute(config.UPDATE_MEANINGFUL_FIELDS_QUERY, *params)

        # TODO: можно одновременно с прошлым запросом делать
        await db.execute(
            f"UPDATE {config.result_table_name} SET flag_prellm = TRUE WHERE person_id = $1",
            person_id
        )

        logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] ✅ prellm завершен успешно")

    except Exception as e:
        await db.execute(
            f"UPDATE {config.result_table_name} SET flag_prellm = FALSE WHERE person_id = $1",
            person_id
        )
        logger.exception(f"[Воркер #{worker_id}][person_id={person_id}] ❌ Ошибка: {e}")
        raise
    finally:
        await db.close()
