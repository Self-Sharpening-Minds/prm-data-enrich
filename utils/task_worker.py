import asyncio
import logging

import config
from handlers import llm, perp, postcheck1, postcheck2, prellm
from utils.db import AsyncDatabaseManager
from services.fill_task_queue import TaskQueue

logger = logging.getLogger(__name__)

HANDLERS = {
    "prellm": prellm.run,
    "llm": llm.run,
    "perp": perp.run,
    "postcheck1": postcheck1.run,
    "postcheck2": postcheck2.run,
    # "photos": photos.run
}

NEXT_TASK = {
    "prellm": "llm",
    "llm": "perp",
    "perp": "postcheck1",
    "postcheck1": "postcheck2",
    "postcheck2": None, #"postcheck2": "photos",
    #"photos": None
}

async def fetch_pending_task(db: AsyncDatabaseManager) -> dict | None:
    """
    Извлекает одну задачу из очереди со статусом 'pending',
    помечает её как 'in_progress' и возвращает словарь с данными задачи.

    Returns:
        dict | None: словарь с параметрами задачи или None, если задач нет.
    """
    rows = await db.fetch(config.TAKE_PENDING_TASK_QUERY)
    if not rows:
        logger.debug("Нет доступных задач в очереди.")
        return None
    return rows[0]


async def create_new_task(person_id: int, current_task: str) -> None:
    """
    Создаёт следующую задачу для персоны на основе текущей.

    Args:
        person_id: ID персоны
        current_task: завершённая задача
    """
    next_task = NEXT_TASK.get(current_task)

    if not next_task:
        logger.debug(f"[person_id={person_id}] Нет следующей задачи после {current_task}")
        return

    queue = TaskQueue()
    await queue.add_for_person(person_id, next_task)


async def mark_task_status(db: AsyncDatabaseManager, task_id: int, status: bool, error: str | None = None) -> None:
    """
    Обновляет статус задачи в таблице task_queue.

    Args:
        db: Подключение к БД.
        task_id: Идентификатор задачи.
        status: Новый статус ('done', 'failed').
        error: Текст ошибки, если есть.
    """
    if status:
        query = "UPDATE task_queue SET status='done', finished_at=NOW() WHERE id=$1"
        await db.execute(query, task_id)
        logger.debug(f"Задача {task_id} помечена как 'done'.")
    else:
        query = """
            UPDATE task_queue
            SET status='failed', finished_at=NOW(), retries=retries+1, last_error=$1
            WHERE id=$2
        """
        await db.execute(query, error or "Unknown error", task_id)
        logger.debug(f"Задача {task_id} помечена как 'failed': {error}")


async def run_handler(worker_id: int, task_type: str, person_id: int) -> bool:
    """
    Вызывает соответствующий обработчик для указанного типа задачи.

    Args:
        worker_id: Идентификатор воркера.
        task_type: Тип задачи (prellm, llm, perp и т. д.).
        person_id: ID персоны.
    """
    handler = HANDLERS.get(task_type)
    if not handler:
        raise ValueError(f"Неизвестный тип задачи: {task_type}")

    logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] Запуск обработчика '{task_type}'")
    status = await handler(worker_id, person_id)
    logger.debug(f"[Воркер #{worker_id}][person_id={person_id}] Завершён обработчик '{task_type}'")
    return status


async def process_task(db: AsyncDatabaseManager, task: dict, worker_id: int) -> None:
    """
    Выполняет одну задачу, включая обработку ошибок и обновление статуса в БД.

    Args:
        db: Подключение к БД.
        task: Словарь с параметрами задачи.
        worker_id: Идентификатор воркера.
    """
    task_id = task["id"]
    person_id = task["person_id"]
    task_type = task["task_type"]

    try:
        status = await run_handler(worker_id, task_type, person_id)
        await mark_task_status(db, task_id, True) #TODO: сделать нормальные return в handlers
        if status:
            await create_new_task(person_id, task_type)
        logger.info(f"[Воркер #{worker_id}][person_id={person_id}] ✅ Задача {task_type} завершена успешно")
    except Exception as e:
        logger.exception(f"[Воркер #{worker_id}] ❌ Ошибка при выполнении {task_type}: {e}")
        await mark_task_status(db, task_id, False, str(e))


async def worker_loop(worker_id: int, db: AsyncDatabaseManager) -> None:
    """
    Основной цикл выполнения задач воркером.
    Цикл работает бесконечно, проверяя очередь каждые 5 секунд.

    Args:
        worker_id: Идентификатор воркера.
        db: Подключение к базе данных.
    """
    logger.info(f"🚀 Воркер #{worker_id} запущен")

    while True:
        try:
            task = await fetch_pending_task(db)
            if not task:
                await asyncio.sleep(3)
                return #TODO: должен быть continue

            await process_task(db, task, worker_id)
        except Exception as e:
            logger.exception(f"[Воркер #{worker_id}] Ошибка в основном цикле: {e}")
            await asyncio.sleep(5)
