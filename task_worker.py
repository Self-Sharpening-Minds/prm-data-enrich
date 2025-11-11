import asyncio
import logging
from logger import setup_logging
from utils.db import AsyncDatabaseManager
from handlers import prellm, llm, perp, postcheck1, postcheck2
from services.fill_task_queue import fill_task_queue

setup_logging(level=logging.INFO)
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
    query = """
        UPDATE task_queue
        SET status = 'in_progress', started_at = NOW()
        WHERE id = (
            SELECT id FROM task_queue
            WHERE status = 'pending' and task_type = 'postcheck2'
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING id, person_id, task_type;
    """
    rows = await db.fetch(query)
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
        logger.info(f"[Воркер #{worker_id}][person_id={person_id}] ▶️ Выполнение задачи {task_type}")
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


async def main():
    workers = 1
    db = AsyncDatabaseManager()
    await db.connect()
    await fill_task_queue()
    await asyncio.sleep(3)

    tasks = [worker_loop(i, db) for i in range(workers)]
    try:
        await asyncio.gather(*tasks)
    finally:
        await db.close()
        logger.info("Все воркеры завершили работу")


if __name__ == "__main__":
    asyncio.run(main())
