import logging

import config
from utils import cleaner
from utils.db import AsyncDatabaseManager

logger = logging.getLogger(__name__)


async def fetch_person(db: AsyncDatabaseManager, person_id: int) -> dict | None:
    """
    Получает данные конкретного человека из базы данных.

    Args:
        db: Асинхронный менеджер базы данных.
        person_id: ID персоны.

    Returns:
        dict | None: Словарь с данными персоны или None, если не найдено.
    """
    query = f"""
        SELECT person_id, first_name, last_name, about,
               personal_channel_title, personal_channel_about
        FROM {config.result_table_name}
        WHERE person_id = $1
    """
    rows = await db.fetch(query, person_id)
    if not rows:
        logger.warning(f"[person_id={person_id}] ⚠️ Не найден в БД")
        return None
    return rows[0]


def normalize_person_fields(person: dict) -> None:
    """
    Приводит поля персоны к нормализованной форме.

    Args:
        person: Словарь с данными персоны.
    """
    fields_to_normalize = [
        'first_name', 'last_name',
        'about', 'personal_channel_title',
        'personal_channel_about'
    ]
    for field in fields_to_normalize:
        person[field] = cleaner.normalize_empty(person.get(field))
    logger.debug(f"[person_id={person.get('person_id')}] Поля нормализованы")


def extract_meaningful_data(person: dict) -> tuple[str | None, str | None, str | None, list[str]]:
    """
    Извлекает и очищает meaningful-поля для LLM.

    Args:
        person: Словарь с данными персоны.

    Returns:
        tuple: first_name, last_name, about_clean, extracted_links
    """
    first_name = cleaner.clean_name_field(person.get('first_name'))
    last_name = cleaner.clean_second_name_field(person.get('last_name'))
    about = person.get('about')
    channel_title = person.get('personal_channel_title')
    channel_about = person.get('personal_channel_about')

    if first_name and ' ' in first_name and not last_name:
        parts = first_name.split(' ', 1)
        if len(parts) == 2:
            first_name, last_name = parts

    about_clean = cleaner.merge_about_fields(about, channel_title, channel_about)
    extracted_links = cleaner.extract_links(last_name, about, channel_about)

    logger.debug(
        f"[person_id={person.get('person_id')}] "
        f"first_name={first_name}, last_name={last_name}, "
        f"about_clean={'Есть' if about_clean else 'None'}, links_count={len(extracted_links)}"
    )
    return first_name, last_name, about_clean, extracted_links


async def update_meaningful_fields(
    db: AsyncDatabaseManager,
    person_id: int,
    first_name: str | None,
    last_name: str | None,
    about_clean: str | None,
    extracted_links: list[str]
) -> None:
    """
    Обновляет meaningful-поля и ставит флаг prellm.

    Args:
        db: Асинхронный менеджер базы данных.
        person_id: ID персоны.
        first_name: Очищенное имя.
        last_name: Очищенная фамилия.
        about_clean: Объединённое поле about.
        extracted_links: Список ссылок.
    """
    await db.execute(config.UPDATE_MEANINGFUL_FIELDS_QUERY, first_name, last_name, about_clean, extracted_links, person_id)
    await db.execute(
        f"UPDATE {config.result_table_name} SET flag_prellm = TRUE WHERE person_id = $1",
        person_id
    )
    # logger.debug(f"[person_id={person_id}] Поля meaningful обновлены и flag_prellm установлен")


async def run(worker_id: int, person_id: int) -> bool:
    """
    Асинхронная предварительная очистка данных перед обработкой LLM.

    Args:
        worker_id: ID воркера для логирования.
        person_id: ID персоны для обработки.
    """
    # logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] Начинаем prellm")

    db = AsyncDatabaseManager()
    await db.connect()

    try:
        person = await fetch_person(db, person_id)
        if not person:
            return False

        normalize_person_fields(person)
        first_name, last_name, about_clean, extracted_links = extract_meaningful_data(person)
        await update_meaningful_fields(db, person_id, first_name, last_name, about_clean, extracted_links)
        return True
        # logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] ✅ prellm завершен успешно")

    except Exception as e:
        logger.exception(f"[Воркер #{worker_id}][person_id={person_id}] ❌ Ошибка prellm: {e}")
        await db.execute(
            f"UPDATE {config.result_table_name} SET flag_prellm = FALSE WHERE person_id = $1",
            person_id
        )
        raise
    finally:
        await db.close()
