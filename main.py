import argparse
import asyncio
import datetime
import logging
from pathlib import Path
from typing import Any
import base64
import mimetypes

import config
from jinja2 import Environment, FileSystemLoader
from llm.llm_client import LlmClient
from llm.perp_client import PerplexityClient
from logger import setup_logging
from utils import cleaner
from utils.db import DatabaseManager
from utils.md_exporter import MarkdownExporter
from utils.photo_processor import PhotoProcessor

setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_and_create_db() -> None:
    """Очищает исходную базу данных и создает новые рабочие таблицы.

    Удаляет и пересоздает таблицы `cleaned_table_name` и `result_table_name`
    на основе конфигурации.
    """
    logger.info("Начинаем очистку базы данных и создание новых таблиц.")
    db = DatabaseManager()
    try:
        db.create_cleaned_table(
            source_table_name=config.source_table_name,
            new_table_name=config.cleaned_table_name
        )
        db.create_result_table(
            source_table_name=config.cleaned_table_name,
            result_table_name=config.result_table_name,
            drop_table=True
        )
    finally:
        db.close()
    logger.info("База данных успешно подготовлена.")


def pre_llm() -> None:
    """Выполняет предварительную очистку данных перед обработкой LLM.

    Извлекает необработанные данные, применяет к ним функции очистки
    (удаление мусора, нормализация) и обновляет "meaningful" поля в базе данных.
    """
    db = DatabaseManager()
    try:
        select_query = f"""
            SELECT person_id, first_name, last_name, about,
                   personal_channel_title, personal_channel_about
            FROM {config.result_table_name} ORDER BY person_id
        """
        persons = db.execute_query(select_query)
        fields_to_normalize = [
            'person_id', 'first_name','last_name',
            'about', 'personal_channel_title', 'personal_channel_about'
        ]

        for person in persons:
            for field in fields_to_normalize:
                person[field] = cleaner.normalize_empty(person.get(field))

            person_id = person.get('person_id')
            first_name = cleaner.clean_name_field(person.get('first_name'))
            last_name = cleaner.clean_second_name_field(person.get('last_name'))
            about = person.get('about')
            channel_title = person.get('personal_channel_title')
            channel_about = person.get('personal_channel_about')

            person_extracted_links = cleaner.extract_links(
                last_name,
                about,
                channel_about
            )

            if first_name and ' ' in first_name and not last_name:
                parts = first_name.split(' ', 1)
                if len(parts) == 2:
                    first_name, last_name = parts

            about_clean = cleaner.merge_about_fields(about, channel_title, channel_about)
            params = (first_name, last_name, about_clean, person_extracted_links, person_id)
            db.execute_query(config.UPDATE_MEANINGFUL_FIELDS_QUERY, params)
    finally:
        db.close()
    logger.info("✅ Предварительная обработка завершена.")


def export_batch_to_db(db: DatabaseManager, parsed_chunk: dict[str, dict[str, Any]]) -> int:
    """Сохраняет результаты обработки одного батча от LLM в базу данных.

    Args:
        db: Экземпляр DatabaseManager для выполнения запросов.
        parsed_chunk: Словарь с результатами от LLM, где ключ - индекс,
                      а значение - словарь с данными о человеке.

    Returns:
        Количество успешно обновленных строк в базе данных.
    """
    updated_rows_count = 0
    for data in parsed_chunk.values():
        if not isinstance(data, dict):
            logger.warning(f"Пропуск элемента: ожидался dict, получен {type(data)}")
            continue

        person_id = data.get('person_id')
        if not person_id:
            logger.warning("Пропуск элемента: отсутствует 'person_id'.")
            continue

        first_name = data.get('meaningful_first_name')
        last_name = data.get('meaningful_last_name')
        about = data.get('meaningful_about')

        #TODO: может не здесь нужно сразу расставлять valid
        person_extracted_links = db.execute_query(f"SELECT extracted_links FROM {config.result_table_name} WHERE person_id={person_id}")
        extracted_links = person_extracted_links[0].get("extracted_links", None)

        is_valid = bool(first_name and last_name and (about or extracted_links))
        params = (first_name, last_name, about, is_valid, person_id)

        try:
            result = db.execute_query(config.UPDATE_LLM_RESULTS_QUERY, params)
            affected_rows = result[0].get('affected_rows', 0) if result else 0
            if affected_rows > 0:
                updated_rows_count += 1
            else:
                logger.warning(f"Строка для person_id {person_id} не была обновлена в БД.")
        except Exception as e:
            logger.error(f"Ошибка обновления БД для person_id {person_id}: {e}")

    return updated_rows_count


async def process_chunk(
    llm: LlmClient,
    db: DatabaseManager,
    chunk_to_process: dict[int, dict[str, Any]],
    chunk_index: int
) -> bool:
    """
    Обрабатывает один чанк данных с логикой повторных попыток.
    Возвращает True в случае успеха, False в случае неудачи.
    """
    for attempt in range(config.MAX_RETRIES):
        logger.info(f"Обработка чанка #{chunk_index}. Попытка {attempt + 1}/{config.MAX_RETRIES}.")

        try:
            parsed_chunk = await llm.async_parse_chunk_to_meaningful(chunk_to_process)

            if not isinstance(parsed_chunk, dict) or not parsed_chunk:
                logger.warning(
                    f"LLM вернула некорректный результат для чанка #{chunk_index}. "
                    f"Тип: {type(parsed_chunk)}"
                )
                await asyncio.sleep(0.5)
                continue

            # синхронная функция БД в отдельном потоке, чтобы не блокировать #TODO
            updated_count = await asyncio.to_thread(export_batch_to_db, db, parsed_chunk)

            if updated_count == len(chunk_to_process):
                logger.info(
                    f"✅ Чанк #{chunk_index} успешно обработан и сохранен в БД "
                    f"({updated_count} строк)."
                )
                return True
            else:
                logger.warning(
                    f"БД обработала чанк #{chunk_index} не полностью "
                    f"(обновлено {updated_count}/{len(chunk_to_process)}). "
                )
                await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(f"Ошибка при обработке чанка #{chunk_index} на попытке {attempt + 1}: {e}", exc_info=True)
            await asyncio.sleep(1)

    logger.error(f"❌ Не удалось обработать чанк #{chunk_index} после {config.MAX_RETRIES} попыток. Пропускаем.")
    return False


async def test_llm(start_position: int, row_count: int) -> None:
    """(async) Обрабатывает записи партиями (батчами) через LLM для очистки данных.

    Функция выбирает записи из `result_table_name`, формирует из них батчи
    и отправляет в LlmClient для извлечения осмысленных полей
    (имя, фамилия, описание). Результаты сохраняются обратно в БД.
    Реализован механизм повторных попыток для неудачно обработанных батчей.

    Args:
        start_position: Начальная позиция (OFFSET) для выборки записей из БД.
        row_count: Количество записей (LIMIT) для обработки. Если -1, обрабатываются все.
    """
    logger.info("Начинаем обработку записей через LLM.")

    select_query = (
        f"{config.SELECT_PERSONS_BASE_QUERY} "
        f"ORDER BY person_id"
    )
    if row_count > 0: select_query += f" LIMIT {row_count}"
    if start_position > 0: select_query += f" OFFSET {start_position}"

    db = DatabaseManager()
    try:
        records = db.execute_query(select_query)
        total = len(records)
        if total == 0:
            logger.info("Нет записей для обработки.")
            return

        llm = LlmClient()
        CHUNK_SIZE: int = config.CHUNK_SIZE

        all_chunks = []
        for i in range(0, total, CHUNK_SIZE):
            right_index = min(i + CHUNK_SIZE, total)
            chunk = {
                index: {
                    "person_id": row.get('person_id'),
                    "first_name": row.get('meaningful_first_name', ''),
                    "last_name": row.get('meaningful_last_name', ''),
                    "about": row.get('meaningful_about', '')
                } for index, row in enumerate(records[i:right_index])
            }
            all_chunks.append(chunk)

        logger.info(f"Подготовлено {len(all_chunks)} чанков для обработки.")

        semaphore = asyncio.Semaphore(config.ASYNC_LLM_REQUESTS_WORKERS)

        async def worker_with_semaphore(chunk, index):
            async with semaphore:
                return await process_chunk(llm, db, chunk, index)

        tasks = [
            worker_with_semaphore(chunk, i)
            for i, chunk in enumerate(all_chunks)
        ]

        results = await asyncio.gather(*tasks)

        successful_chunks = sum(1 for res in results if res)
        logger.info(
            f"✅ Обработка завершена. Успешно обработано: "
            f"{successful_chunks}/{len(all_chunks)} чанков."
        )

    finally:
        db.close()
    logger.info("✅ Обработка записей через LLM завершена.")


def export_person_to_md(
        person: dict[str, Any], exporter: MarkdownExporter,
        summary: str | None, urls: list
    ) -> str | None:
    """
    Экспорт человека в Markdown.
    """
    filepath = exporter.export_to_md(
            first_name=person.get("meaningful_first_name", "Unknown"),
            last_name=person.get("meaningful_last_name", "Unknown"),
            content=summary or '',
            urls=urls,
            personal_channel=person.get('personal_channel_username', '')
        )

    if filepath:
        logger.info(f"✅ Создан файл: {filepath}")
        return filepath
    else:
        logger.error("❌ Ошибка создания файла")
        return


async def process_person_for_search(
    person: dict,
    perp_client: PerplexityClient,
    check_llm: LlmClient,
    db: DatabaseManager,
    exporter: MarkdownExporter | None
) -> None:
    """
    Выполняет полный цикл поиска и сохранения информации для одной персоны.
    """
    person_id = person.get('person_id')
    try:
        logger.info(f"Начинаем поиск для person_id: {person_id}")
        search_result = await perp_client.async_search_info(
            first_name=person.get("meaningful_first_name", ""),
            last_name=person.get("meaningful_last_name", ""),
            about=person.get("meaningful_about", ""),
            extracted_links=person.get("extracted_links", [])
        )

        summary = search_result.get("summary")
        if not summary: summary = ''
        is_summary_valid = await check_llm.async_postcheck(summary)

        params = (
            summary if is_summary_valid else None,
            search_result.get("urls"),
            search_result.get("confidence") if is_summary_valid else "low",
            person_id
        )
        await asyncio.to_thread(db.execute_query, config.UPDATE_SUMMARY_QUERY, params)

        if exporter and is_summary_valid:
            await asyncio.to_thread(
                export_person_to_md,
                person=person,
                exporter=exporter,
                summary=summary,
                urls=search_result.get("urls", [])
            )
        logger.info(f"✅ Успешно обработан person_id: {person_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка при обработке person_id {person_id}: {e}", exc_info=True)


async def test_perpsearch(start_position: int, row_count: int, md_flag: bool) -> None:
    """(async) Выполняет поиск информации о персонах через Perplexity и сохраняет результаты.

    Для каждой "валидной" персоны из БД формируется поисковый запрос.
    Найденная информация (summary, urls) и оценка уверенности (confidence)
    проходят дополнительную проверку через LlmClient и сохраняются в БД.
    При установленном флаге `md_flag` результаты также экспортируются в Markdown.

    Args:
        start_position: Начальная позиция (OFFSET) для выборки записей.
        row_count: Количество записей (LIMIT) для обработки.
        md_flag: Флаг, разрешающий экспорт результатов в Markdown файлы.
    """
    logger.info("Начинаем поиск информации через PerplexityClient.")

    select_query = (
        f"{config.SELECT_PERSONS_BASE_QUERY} "
        f"WHERE valid ORDER BY person_id"
    )
    if row_count > 0: select_query += f" LIMIT {row_count}"
    if start_position > 0: select_query += f" OFFSET {start_position}"

    db = DatabaseManager()

    try:
        persons = db.execute_query(select_query)
        total = len(persons)
        if total == 0:
            logger.info("Не найдено валидных персон для поиска информации.")
            return

        perp_client = PerplexityClient()
        check_llm = LlmClient()
        exporter = None
        if md_flag:
            date_str = datetime.datetime.now().strftime("%Y-%m-%d-%H%M")
            exporter = MarkdownExporter(f"data/{date_str}_person_reports")

        semaphore = asyncio.Semaphore(config.ASYNC_SEARCH_REQUESTS_WORKERS)

        async def worker_with_semaphore(person):
            async with semaphore:
                await process_person_for_search(person, perp_client, check_llm, db, exporter)

        tasks = [worker_with_semaphore(person) for person in persons]

        await asyncio.gather(*tasks)

        logger.info(f"Обработано {total} записей.")

    finally:
        db.close()
    logger.info("✅ Поиск информации завершен.")


def test_searching_photos() -> None:
    """
    Ищет, анализирует и кластеризует фото из веба и файлов.
    Если кластер не найден, сохраняет локальные фото с лицами.
    """
    logger.info("Начинаем поиск и анализ фотографий.")

    select_query = f"""
        {config.SELECT_PERSONS_BASE_QUERY}
        WHERE valid AND summary IS NOT NULL AND TRIM(summary) != ''
        ORDER BY person_id
    """

    db = DatabaseManager()
    try:
        persons = db.execute_query(select_query)
        if not persons:
            logger.info("Не найдено персон для поиска фотографий.")
            return

        photo_processor = PhotoProcessor()
        total = len(persons)

        for i, person in enumerate(persons, 1):
            person_id = person.get("person_id")
            person_urls = person.get("urls", [])

            logger.info(f"[{i}/{total}] Обработка фотографий для person_id: {person_id}")

            web_image_urls = []
            if person_urls:
                for url in person_urls:
                    web_image_urls.extend(photo_processor.extract_image_urls_from_page(url))
                    if len(web_image_urls) > 50:
                        web_image_urls = [] #TODO: когда слишком много фоток, алгоритм тупит
                logger.debug(f"Найдено {len(web_image_urls)} изображений в вебе для person_id: {person_id}")

            local_avatars = []
            avatars_dir = Path(config.PATH_PRM_MEDIA) / str(person_id) / config.PATH_PERSON_TG_AVATARS
            if avatars_dir.is_dir():
                found_files = list(avatars_dir.glob('*.jpg'))
                local_avatars = [str(p) for p in found_files]
                logger.debug(f"Найдено {len(local_avatars)} локальных аватаров в {avatars_dir} для person_id: {person_id}")

            web_human_face_images = [
                url for url in set(web_image_urls)
                if photo_processor.is_single_human_face(url)
            ]

            local_human_face_images = [
                path for path in set(local_avatars)
                if photo_processor.is_single_human_face(path)
            ]

            all_human_face_images = web_human_face_images + local_human_face_images
            
            if not all_human_face_images:
                logger.warning(f"❌ Найдено {len(web_human_face_images)} веб-фото с лицом и {len(local_human_face_images)} локальных фото с лицом для person_id: {person_id}.")
                continue

            logger.info(f"Найдено {len(web_human_face_images)} веб-фото с лицом и {len(local_human_face_images)} локальных фото с лицом для person_id: {person_id}.")

            clusters = photo_processor.cluster_faces(all_human_face_images)
            if not clusters:
                logger.warning("❌ Кластеры не сформированы. Проверяем наличие локальных фото с лицами.")
                if local_human_face_images:
                    logger.info(f"❌✅ Сохраняем {len(local_human_face_images)} локальных фото с лицами как запасной вариант.")
                    params = (local_human_face_images, person_id)
                    db.execute_query(config.UPDATE_PHOTOS_QUERY, params)
                else:
                    logger.info("❌❌ Локальных фото с лицами для сохранения не найдено.")
                continue

            main_cluster = max(clusters, key=len)
            if len(main_cluster) >= config.MIN_PHOTOS_IN_CLUSTER:
                params = (main_cluster, person_id)
                db.execute_query(config.UPDATE_PHOTOS_QUERY, params)
                logger.info(
                    f"✅ Для {person_id} найден и сохранен кластер из {len(main_cluster)} фотографий."
                )
            else:
                logger.warning(f"Самый большой кластер ({len(main_cluster)} фото) слишком мал. Проверяем локальные фото.")
                if local_human_face_images:
                    logger.info(f"Сохраняем {len(local_human_face_images)} локальных фото с лицами вместо маленького кластера.")
                    params = (local_human_face_images, person_id)
                    db.execute_query(config.UPDATE_PHOTOS_QUERY, params)
    finally:
        db.close()
    logger.info("Поиск и анализ фотографий завершен.")


def get_pipeline_stats() -> dict:
    """
    Собирает статистику по воронке обработки персон с помощью одного SQL-запроса.

    Возвращает словарь со следующими метриками:
    - total_persons: Всего персон в таблице.
    - valid_persons: Прошли первичную валидацию LLM (есть имя, фамилия и meaningful_about).
    - with_searchable_info_only: Валидные, у которых есть meaningful_about, но нет ссылок.
    - with_links_only: Валидные, у которых есть ссылки, но нет meaningful_about.
    - with_both_info_and_links: Валидные, у которых есть и meaningful_about, и ссылки.
    - ready_for_html: Финальное количество персон с найденным summary, готовых к экспорту.
    """
    logger.info("Сбор статистики по воронке обработки...")
    
    db = DatabaseManager()
    stats = {}

    stats_query = f"""
        SELECT
            COUNT(*) AS total_persons,
            
            COUNT(CASE WHEN valid THEN 1 END) AS valid_persons,
            
            COUNT(CASE WHEN valid 
                          AND (meaningful_about IS NOT NULL AND TRIM(meaningful_about) != '')
                          AND (extracted_links IS NOT NULL AND array_length(extracted_links, 1) is null)
                     THEN 1 END) AS with_meaningful_about_only,
            
            COUNT(CASE WHEN valid
                          AND (meaningful_about IS NULL OR TRIM(meaningful_about) = '')
                          AND (extracted_links IS NOT NULL AND array_length(extracted_links, 1) > 0)
                     THEN 1 END) AS with_links_only,

            COUNT(CASE WHEN valid
                          AND (meaningful_about IS NOT NULL AND TRIM(meaningful_about) != '')
                          AND (extracted_links IS NOT NULL AND array_length(extracted_links, 1) > 0)
                     THEN 1 END) AS with_both_about_and_links,

            COUNT(CASE WHEN valid
                          AND (summary IS NOT NULL AND TRIM(summary) != '')
                     THEN 1 END) AS ready_for_html

        FROM {config.result_table_name};
    """
    
    try:
        result = db.execute_query(stats_query)
        if result:
            stats = result[0]
        else:
            logger.warning("Не удалось получить статистику, запрос вернул пустой результат.")
            return {}

    finally:
        db.close()

    logger.info("Статистика успешно собрана.")
    logger.info("\n--- Статистика воронки обработки ---")
    for key, value in stats.items():
        logger.info(f"{key:<30}: {value}")
    logger.info("------------------------------------")
    return stats


def export_to_html() -> None:
    """
    Экспортирует данные о персонах из БД в единый HTML-файл,
    используя шаблонизатор Jinja2 для генерации разметки.
    """
    logger.info("Начинаем экспорт людей в html таблицу.")
    db = DatabaseManager()
    try:
        select_query: str = f"""
            {config.SELECT_PERSONS_BASE_QUERY}
            WHERE valid AND summary IS NOT NULL
            AND TRIM(summary) != '' ORDER BY person_id
        """
        persons = db.execute_query(select_query)
    finally:
        db.close()

    if not persons:
        logger.warning("Не найдено персон для экспорта.")
        return

    try:
        env = Environment(loader=FileSystemLoader('templates/'), autoescape=True)
        template = env.get_template('template.html')
        css_content = Path('templates/style.css').read_text(encoding='utf-8')

        for person in persons:
            person['summary'] = cleaner.clean_summary(person.get('summary', ''))
            photo_sources = person.get('photos') or []
            local_photos = []
            web_photos = []
            for src in photo_sources:
                if src and src.startswith('prm_media/'):
                    try:
                        file_path = Path(src)
                        mime_type, _ = mimetypes.guess_type(file_path)
                        if not mime_type: mime_type = 'image/jpeg'
                    
                        encoded_data = base64.b64encode(file_path.read_bytes()).decode('ascii')
                        base64_uri = f"data:{mime_type};base64,{encoded_data}"
                        if base64_uri:
                            local_photos.append(base64_uri)
                    except FileNotFoundError:
                        logger.warning(f"Локальный файл не найден, пропуск: {src}")
                    except Exception as e:
                        logger.error(f"Ошибка кодирования файла {src}: {e}")
                elif src:
                    web_photos.append(src)
            person['web_photos'] = web_photos
            person['local_photos'] = local_photos

        final_html = template.render(
            people=persons,
            css_content=css_content
        )

        result_filename = "people_analysis.html"
        Path(result_filename).write_text(final_html, encoding='utf-8')
        logger.info(f"✅ HTML-таблица успешно сохранена в файл: {result_filename}")

    except FileNotFoundError as e:
        logger.error(f"Ошибка: файл шаблона или стилей не найден: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при создании HTML: {e}")


async def main() -> None:
    """
    Главная функция для запуска утилиты из командной строки.
    """
    parser = argparse.ArgumentParser(description="Инструменты для поиска информации с помощью LLM.")
    parser.add_argument("--dbcreate", action="store_true",
                        help="Очистка и подготовка базы данных"
    )
    parser.add_argument("--prellm", action="store_true",
                        help="Предобработка данных"
    )
    parser.add_argument("--llm", action="store_true",
                        help="Обработка записей через LLM"
    )
    parser.add_argument("--perp", action="store_true",
                        help="Поиск информации и экспорт в Markdown"
    )
    parser.add_argument("--stats", action="store_true",
                        help="Анализ воронки"
    )
    parser.add_argument("--photos", action="store_true",
                        help="Поиск фотографий из ссылок"
    )
    parser.add_argument("--start", type=int, default=0,
                        help="Начальная позиция записи"
    )
    parser.add_argument("--count", type=int, default=-1,
                        help="Количество записей"
    )
    parser.add_argument("--md", action="store_true", default=False,
                        help="Разрешить экспорт в md"
    )
    parser.add_argument("--html", action="store_true",
                        help="Экспорт в html таблицу"
    )
    parser.add_argument("--all", action="store_true",
                        help="Полный проход"
    )
    args = parser.parse_args()

    if args.dbcreate:
        clean_and_create_db()
    elif args.prellm:
        pre_llm()
    elif args.llm:
        await test_llm(start_position=args.start, row_count=args.count)
    elif args.perp:
        await test_perpsearch(start_position=args.start, row_count=args.count, md_flag=args.md)
        pipeline_stats = get_pipeline_stats()
    elif args.photos:
        test_searching_photos()
    elif args.html:
        export_to_html()
    elif args.stats:
        get_pipeline_stats()
    elif args.all:
        clean_and_create_db()
        pre_llm()
        await test_llm(start_position=args.start, row_count=args.count)
        await test_perpsearch(start_position=args.start, row_count=args.count, md_flag=args.md)
        # test_searching_photos()
        export_to_html()
        get_pipeline_stats()
    else:
        parser.print_help()


if __name__ == "__main__":
    asyncio.run(main())
