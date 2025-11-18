import argparse
import asyncio
import base64
import json
import logging
import mimetypes
from pathlib import Path
from typing import Any

import config
from jinja2 import Environment, FileSystemLoader
from logger import setup_logging
from services.fill_task_queue import TaskQueue
from utils import cleaner
from utils.db import AsyncDatabaseManager
from utils.task_worker import worker_loop

logger = logging.getLogger(__name__)


async def _get_db() -> AsyncDatabaseManager:
    """Создаёт подключение к базе данных и возвращает менеджер."""
    db = AsyncDatabaseManager()
    await db.connect()
    logger.debug("Соединение с базой данных установлено.")
    return db


def _prepare_environment() -> tuple[Environment, str]:
    """Подготавливает Jinja2 Environment и загружает CSS-шаблон."""
    env = Environment(loader=FileSystemLoader('templates/'), autoescape=True)
    css_path = Path('templates/style.css')
    css_content = css_path.read_text(encoding='utf-8')
    logger.debug(f"CSS успешно загружен из {css_path}")
    return env, css_content


def _process_person_photos(photo_sources: list[str]) -> tuple[list[str], list[str]]:
    """Разделяет и кодирует локальные и веб-фото для экспорта."""
    local_photos, web_photos = [], []
    for src in photo_sources or []:
        if not src:
            continue
        if src.startswith('prm_media/'):
            try:
                file_path = Path(src)
                mime, _ = mimetypes.guess_type(file_path)
                mime = mime or 'image/jpeg'
                encoded = base64.b64encode(file_path.read_bytes()).decode('ascii')
                local_photos.append(f"data:{mime};base64,{encoded}")
                logger.debug(f"Локальное фото добавлено: {src}")
            except FileNotFoundError:
                logger.warning(f"Пропущено — файл не найден: {src}")
            except Exception as e:
                logger.error(f"Ошибка при обработке файла {src}: {e}")
        else:
            web_photos.append(src)
            logger.debug(f"Веб-фото добавлено: {src}")
    return local_photos, web_photos


async def clean_and_create_db() -> None:
    """Очищает и пересоздаёт основные таблицы проекта."""
    logger.info("🔄 Подготовка баз данных...")
    db = await _get_db()
    try:
        for query in (
            config.DROP_AND_CREATE_CLEANED_TABLE_QUERY,
            config.DROP_AND_CREATE_RESULT_TABLE_QUERY,
            config.DROP_AND_CREATE_TASK_QUEUE_QUERY,
        ):
            logger.debug(f"Выполнение SQL:\n{query}")
            await db.execute(query)
        logger.info("✅ Таблицы успешно пересозданы")
    finally:
        await db.close()
        logger.debug("Соединение с БД закрыто.")


async def get_pipeline_stats() -> dict[str, Any]:
    """Возвращает статистику по статусам обработки."""
    logger.info("Сбор статистики по флагам этапов...")
    db = await _get_db()
    try:
        logger.debug(f"SQL-запрос статистики:\n{config.STATS_QUERY}")
        rows = await db.fetch(config.STATS_QUERY)
        stats = rows[0] if rows else {}
    finally:
        await db.close()

    if stats:
        logger.info("----- 📈 Pipeline Stats -----")
        for k, v in stats.items():
            logger.info(f"{k:<25}: {v}")
        logger.info("-----------------------------")
    else:
        logger.warning("⚠️ Статистика пуста")
    return stats


async def export_to_json():
    db = AsyncDatabaseManager()
    await db.connect()
    try:
        query = """SELECT * FROM public.person_result_data WHERE done = TRUE;"""
        persons = await db.fetch(query)

        for person in persons:
            person['fetch_date'] = str(person.get('fetch_date', ''))
            original_summary = person.get('summary', '')
            person['summary'] = cleaner.clean_summary(original_summary)
            person_facts = []
            person_summary = ''

            if original_summary:
                for fact in original_summary[original_summary.find("[") + 1:original_summary.find("]")].strip().split("\","):
                    person_facts.append(fact.replace("\"", "").strip())
                person_summary = original_summary[original_summary.find("summary") + 10:-2].strip()

            person['summary'] = person_summary
            person['new_facts'] = person_facts

        with open('people_analysis.json', 'w', encoding='utf-8') as f:
            json.dump(persons, f, ensure_ascii=False, indent=2)

        logger.info(f"✅ Экспортировано {len(persons)} записей в people_analysis.json")

    except Exception as e:
        logger.error(f"❌ Ошибка при экспорте: {e}")
    finally:
        await db.close()


async def export_to_html() -> None:
    """Экспортирует результаты из БД в HTML-файл."""
    logger.info("Экспорт результатов в HTML...")
    db = await _get_db()
    try:
        logger.debug(f"SQL для экспорта:\n{config.SELECT_DONE_QUERY}")
        persons = await db.fetch(config.SELECT_DONE_QUERY)
    finally:
        await db.close()

    if not persons:
        logger.warning("⚠️ Нет данных для экспорта")
        return

    env, css_content = _prepare_environment()
    template = env.get_template('template.html')

    for person in persons:
        person['summary'] = cleaner.clean_summary(person.get('summary', ''))
        local_photos, web_photos = _process_person_photos(person.get('photos', []))
        person['local_photos'] = local_photos
        person['web_photos'] = web_photos

    result_html = template.render(people=persons, css_content=css_content)
    output_path = Path("people_analysis.html")
    output_path.write_text(result_html, encoding='utf-8')

    logger.info(f"✅ HTML-таблица сохранена: {output_path}")
    logger.debug(f"Количество экспортированных персон: {len(persons)}")


async def run_workers(count: int) -> None:
    """Запускает указанное количество асинхронных воркеров."""
    db = await _get_db()
    queue = TaskQueue()
    await queue.fill_all()
    await asyncio.sleep(2)

    worker_count = count or config.ASYNC_WORKERS
    logger.info(f"🚀 Запуск {worker_count} воркеров...")
    logger.debug(f"Активные обработчики: {worker_count}")

    workers = [worker_loop(i, db) for i in range(worker_count)]
    try:
        await asyncio.gather(*workers)
    finally:
        await db.close()
        logger.info("Все воркеры завершили работу")


async def _run_single_command(args) -> None:
    """Выполняет одну команду CLI в зависимости от аргументов."""
    if args.dbcreate:
        await clean_and_create_db()
    elif args.tasks:
        queue = TaskQueue()
        await queue.fill_all()
    elif args.stats:
        await get_pipeline_stats()
    elif args.html:
        await export_to_html()
    elif args.json:
        await export_to_json()
    elif args.qt:
        await clean_and_create_db()
        await run_workers(2)
        await export_to_html()
        await export_to_json()
    elif args.run > 0:
        await run_workers(args.run)
    else:
        logger.warning("Не указана команда. Используйте --help для справки.")


async def main():
    """CLI-интерфейс для управления пайплайном обработки персон."""
    parser = argparse.ArgumentParser(description="Pipeline Manager for People Processing")
    parser.add_argument("--dbcreate", action="store_true", help="Пересоздать таблицы БД")
    parser.add_argument("--tasks", action="store_true", help="Заполнить очередь задач")
    parser.add_argument("--run", type=int, nargs='?', const=4, default=config.ASYNC_WORKERS, help="Запустить указанное количество воркеров")
    parser.add_argument("--stats", action="store_true", help="Показать статистику по флагам")
    parser.add_argument("--html", action="store_true", help="Экспортировать результаты в HTML")
    parser.add_argument("--json", action="store_true", help="Экспортировать результаты в JSON")
    parser.add_argument("--qt", action="store_true", help="Быстрый тест (для отладки)")

    args = parser.parse_args()
    await _run_single_command(args)


if __name__ == "__main__":
    setup_logging()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Завершение работы...")
