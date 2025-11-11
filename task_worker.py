import asyncio
import logging
from logger import setup_logging
from utils.db import DatabaseManager
from handlers import prellm

setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)

HANDLERS = {
    "prellm": prellm.run,
    # "llm": llm.run,
    # "perp": perp.run,
    # "postcheck1": postcheck1.run,
    # "postcheck2": postcheck2.run,
    # "photos": photos.run
}


async def fetch_task(db: DatabaseManager):
    query = """
        UPDATE task_queue
        SET status = 'in_progress', started_at = NOW()
        WHERE id = (
            SELECT id FROM task_queue
            WHERE status = 'pending'
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING id, person_id, task_type;
    """
    result = db.execute_query(query)
    return result[0]


async def process_task(db: DatabaseManager, task, worker_id: int):
    task_id = task.get('id')
    person_id = task.get('person_id')
    task_type = task.get('task_type')
    handler = HANDLERS.get(task_type)

    if not handler:
        logger.error(f"[Воркер #{worker_id}][person_id={person_id}] Неизвестный тип задачи: {task_type}")
        return

    try:
        logger.info(f"[Воркер #{worker_id}][person_id={person_id}] Выполнение задачи {task_type}")
        await handler(worker_id, person_id)
        db.execute_query(
            "UPDATE task_queue SET status='done', finished_at=NOW() WHERE id=%s",
            (task_id,)
        )
        logger.info(f"[Воркер #{worker_id}][person_id={person_id}] Задача {task_type} выполнена")
    except Exception as e:
        logger.exception(e)
        db.execute_query(
            """
            UPDATE task_queue
            SET status='failed', finished_at=NOW(), retries=retries+1, last_error=%s
            WHERE id=%s
            """,
            (str(e), task_id)
        )


async def worker_loop(worker_id: int):
    db = DatabaseManager()
    logger.info(f"Воркер #{worker_id} запущен")

    try:
        while True:
            task = await fetch_task(db)
            if not task:
                await asyncio.sleep(5)
                continue

            await process_task(db, task, worker_id)
    finally:
        db.close()
        logger.info(f"Воркер #{worker_id} завершил работу")


async def main():
    workers = 2
    tasks = [worker_loop(i) for i in range(workers)]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
