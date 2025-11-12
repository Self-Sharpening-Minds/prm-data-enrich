import argparse
import asyncio
import base64
import logging
import mimetypes
from pathlib import Path

import config
from jinja2 import Environment, FileSystemLoader
from logger import setup_logging
from services.fill_task_queue import fill_task_queue
from utils import cleaner
from utils.db import AsyncDatabaseManager
from utils.task_worker import worker_loop

setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)


async def clean_and_create_db() -> None:
    logger.info("🔄 Подготовка базы данных...")
    db = AsyncDatabaseManager()
    await db.connect()

    try:
        await db.execute(config.DROP_AND_CREATE_CLEANED_TABLE_QUERY)
        await db.execute(config.DROP_AND_CREATE_RESULT_TABLE_QUERY)
        await db.execute(config.DROP_AND_CREATE_TASK_QUEUE_QUERY)
        logger.info("✅ Таблицы успешно пересозданы")
    finally:
        await db.close()


async def get_pipeline_stats() -> dict:
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
    logger.info("Сбор статистики по флагам этапов...")

    db = AsyncDatabaseManager()
    await db.connect()
    try:
        query = config.STATS_QUERY
        rows = await db.fetch(query)
        stats = rows[0] if rows else {}
    finally:
        await db.close()

    if stats:
        logger.info("----- 📈 Pipeline Stats -----")
        for k, v in stats.items():
            logger.info(f"{k:<20}: {v}")
        logger.info("-----------------------------")
    else:
        logger.warning("⚠️ Статистика пуста")
    return stats


async def export_to_html() -> None:
    """
    Экспортирует данные о персонах из БД в единый HTML-файл,
    используя шаблонизатор Jinja2 для генерации разметки.
    """
    logger.info("Экспорт результатов в HTML.")
    db = AsyncDatabaseManager()
    await db.connect()
    try:
        query = config.SELECT_DONE_QUERY
        persons = await db.fetch(query)
    finally:
        await db.close()

    if not persons:
        logger.warning("⚠️ Нет данных для экспорта")
        return

    env = Environment(loader=FileSystemLoader('templates/'), autoescape=True)
    template = env.get_template('template.html')
    css_content = Path('templates/style.css').read_text(encoding='utf-8')

    for person in persons:
        person['summary'] = cleaner.clean_summary(person.get('summary', ''))
        local_photos, web_photos = [], []
        for src in person.get('photos', []) or []:
            if src.startswith('prm_media/'):
                try:
                    file_path = Path(src)
                    mime, _ = mimetypes.guess_type(file_path)
                    mime = mime or 'image/jpeg'
                    encoded = base64.b64encode(file_path.read_bytes()).decode('ascii')
                    local_photos.append(f"data:{mime};base64,{encoded}")
                except FileNotFoundError:
                    logger.warning(f"Пропущено — файл не найден: {src}")
            else:
                web_photos.append(src)
        person['local_photos'] = local_photos
        person['web_photos'] = web_photos

    final_html = template.render(people=persons, css_content=css_content)
    result_filename = "people_analysis.html"
    Path(result_filename).write_text(final_html, encoding='utf-8')
    logger.info(f"✅ HTML-таблица успешно сохранена в файл: {result_filename}")


async def run_workers(count: int) -> None:
    db = AsyncDatabaseManager()
    await db.connect()

    await fill_task_queue()
    await asyncio.sleep(2)

    if count <= 0:
        count = config.ASYNC_WORKERS

    workers = [worker_loop(i, db) for i in range(count)]

    try:
        await asyncio.gather(*workers)
    finally:
        await db.close()
        logger.info("Все воркеры завершили работу")


async def main():
    parser = argparse.ArgumentParser(description="Pipeline Manager for People Processing")
    parser.add_argument("--dbcreate", action="store_true", help="Очистить и пересоздать таблицы")
    parser.add_argument("--tasks", action="store_true", help="Заполнить очередь задач")
    parser.add_argument("--run", type=int, default=0, help="Запустить указанное количество воркеров")
    parser.add_argument("--stats", action="store_true", help="Показать статистику по флагам")
    parser.add_argument("--export", action="store_true", help="Экспортировать результаты в HTML")
    parser.add_argument("--run-pipeline", action="store_true", help="Полный проход пайплайна")
    args = parser.parse_args()

    if args.dbcreate:
        await clean_and_create_db()
    elif args.tasks:
        await fill_task_queue()
    elif args.run > 0:
        await run_workers(args.run)
    elif args.stats:
        await get_pipeline_stats()
    elif args.export:
        await export_to_html()
    else:
        parser.print_help()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Завершение работы...")
