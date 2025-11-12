import asyncio
import logging

import config
from handlers import llm, perp, postcheck1, postcheck2, prellm
from utils.db import AsyncDatabaseManager

logger = logging.getLogger(__name__)

HANDLERS = {
    "prellm": prellm.run,
    "llm": llm.run,
    "perp": perp.run,
    "postcheck1": postcheck1.run,
    "postcheck2": postcheck2.run,
    # "photos": photos.run
}


async def fetch_task(db: AsyncDatabaseManager):
    """Берёт одну задачу из очереди (status='pending') и помечает её как 'in_progress'."""
    rows = await db.fetch(config.TAKE_TASK_IN_PROGRESS_QUERY)
    return rows[0] if rows else None


async def process_task(db: AsyncDatabaseManager, task: dict, worker_id: int):
    """Выполняет задачу с помощью соответствующего handler-а."""
    task_id = task["id"]
    person_id = task["person_id"]
    task_type = task["task_type"]
    handler = HANDLERS.get(task_type)

    if not handler:
        logger.error(f"[Воркер #{worker_id}][person_id={person_id}] ❌ Неизвестный тип задачи: {task_type}")
        return

    try:
        logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] Начало выполнения задачи {task_type}")
        await handler(worker_id, person_id)
        await db.execute(
            "UPDATE task_queue SET status='done', finished_at=NOW() WHERE id=$1",
            task_id
        )
        logger.info(f"[Воркер #{worker_id}][person_id={person_id}] ✅ Задача {task_type} выполнена")
    except Exception as e:
        logger.exception(f"[Воркер #{worker_id}][person_id={person_id}] Ошибка при выполнении: {e}")
        await db.execute(
            """
            UPDATE task_queue
            SET status='failed', finished_at=NOW(), retries=retries+1, last_error=$1
            WHERE id=$2
            """,
            str(e),
            task_id
        )


async def worker_loop(worker_id: int, db: AsyncDatabaseManager):
    """Основной цикл воркера."""
    logger.info(f"🚀 Воркер #{worker_id} запущен")

    while True:
        try:
            task = await fetch_task(db)
            if not task:
                await asyncio.sleep(5)
                continue

            await process_task(db, task, worker_id)
        except Exception as e:
            logger.exception(f"[Воркер #{worker_id}] Ошибка в цикле: {e}")
            await asyncio.sleep(5)
