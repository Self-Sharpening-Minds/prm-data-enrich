import os
import re
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass
class DatabaseConfig:
    """Конфигурация подключения к базе данных"""

    host: str = os.getenv("DB_HOST", "localhost")
    database: str = os.getenv("DB_NAME", "postgres")
    user: str = os.getenv("DB_USER", "postgres")
    password: str | None = os.getenv("DB_PASSWORD")
    port: int = int(os.getenv("DB_PORT", "5432"))


@dataclass
class LlmConfig:
    """Конфигурация подключения к llm"""

    key: str | None = os.getenv("OPENROUTER_API_KEY")
    url: str = os.getenv("LLM_URL", "https://openrouter.ai/api/v1")
    # "qwen/qwen3-14b" "mistralai/ministral-8b" "z-ai/glm-4.5-air" "x-ai/grok-4-fast"
    default_model: str = os.getenv("LLM_DEFAULT_MODEL", "x-ai/grok-4-fast")
    check_model: str = os.getenv("LLM_CHECK_MODEL", "mistralai/ministral-8b")
    perplexity_model: str = os.getenv("LLM_PERPLEXITY_MODEL", "perplexity/sonar")


source_table_name = "person_source_data"
cleaned_table_name = "cleaned_person_source_data"
result_table_name = "person_result_data"

EMOJI_PATTERN = re.compile(r"["
    r"\U0001F600-\U0001F64F"  # эмотиконы
    r"\U0001F300-\U0001F5FF"  # символы и пиктограммы
    r"\U0001F680-\U0001F6FF"  # транспорт и символы карт
    r"\U0001F1E0-\U0001F1FF"  # флаги
    r"\U00002700-\U000027BF"  # разнообразные символы
    r"\U0001F900-\U0001F9FF"  # дополнение к эмодзи
r"]+", flags=re.UNICODE)
URL_PATTERN = re.compile(r'https?://\S+|t\.me/\S+|@[\w_]+')
ENRU_CHARS_PATTERN = re.compile(r'[^A-Za-zА-Яа-яЁё\s-]+')

IMAGE_EXTENSIONS = [".jpg", ".jpeg", ".png"]
MIN_PHOTOS_IN_CLUSTER = 2

PATH_PROMPTS = 'prompts/'
PATH_PRM_MEDIA = 'prm_media/'
PATH_PERSON_TG_AVATARS = 'telegram/avatars/'

MAX_RETRIES = 3
ASYNC_WORKERS = 5

# ----- sql_queries -----
SELECT_PERSONS_BASE_QUERY = f"SELECT * FROM {result_table_name}"
UPDATE_MEANINGFUL_FIELDS_QUERY = f"""
    UPDATE {result_table_name}
    SET meaningful_first_name = $1,
        meaningful_last_name = $2,
        meaningful_about = $3,
        extracted_links = $4
    WHERE person_id = $5
"""
UPDATE_LLM_RESULTS_QUERY = f"""
    UPDATE {result_table_name}
    SET meaningful_first_name = $1,
        meaningful_last_name = $2,
        meaningful_about = $3,
        valid = $4
    WHERE person_id = $5
"""
UPDATE_SUMMARY_QUERY = f"""
    UPDATE {result_table_name}
    SET summary = $1,
        urls = $2,
        confidence = $3
    WHERE person_id = $4
"""
UPDATE_PHOTOS_QUERY = f"""
    UPDATE {result_table_name}
    SET photos = $1
    WHERE person_id = $2
"""
DROP_AND_CREATE_TASK_QUEUE_QUERY = """
    DROP TABLE IF EXISTS task_queue;
    CREATE TABLE IF NOT EXISTS task_queue (
        id SERIAL PRIMARY KEY,
        person_id INTEGER NOT NULL,
        task_type TEXT NOT NULL,               -- prellm, llm, perp, postcheck, postcheck2, photos
        status TEXT NOT NULL DEFAULT 'pending', -- pending, in_progress, done, failed
        created_at TIMESTAMP DEFAULT NOW(),
        started_at TIMESTAMP,
        finished_at TIMESTAMP,
        retries INT DEFAULT 0,
        last_error TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_task_status ON task_queue (status);
    CREATE INDEX IF NOT EXISTS idx_task_person ON task_queue (person_id);
"""
DROP_AND_CREATE_CLEANED_TABLE_QUERY = f"""
    DROP TABLE IF EXISTS {cleaned_table_name};
    CREATE TABLE {cleaned_table_name} AS
    SELECT DISTINCT ON ((data->>'telegram_id')::bigint) *
    FROM {source_table_name}
    WHERE data ? 'about'
    ORDER BY (data->>'telegram_id')::bigint, fetch_date DESC;
"""
DROP_AND_CREATE_RESULT_TABLE_QUERY = f"""
    DROP TABLE IF EXISTS {result_table_name};
    CREATE TABLE {result_table_name} AS
    SELECT
        person_id::bigint AS person_id,
        fetch_date::timestamp without time zone AS fetch_date,
        (data->>'telegram_id')::bigint AS telegram_id,
        data->>'first_name' AS first_name,
        data->>'last_name' AS last_name,
        data->>'birth_date' as birth_date,
        data->>'about' AS about,
        data->>'username' AS username,
        data->'personal_channel'->>'title' AS personal_channel_title,
        data->'personal_channel'->>'username' AS personal_channel_username,
        data->'personal_channel'->>'about' AS personal_channel_about,
        (data->'personal_channel'->>'channel_id')::bigint AS personal_channel_id,
        null::boolean AS flag_prellm,
        null::boolean AS flag_llm,
        null::boolean AS valid,
        null::boolean AS flag_perp,
        null::boolean AS flag_postcheck1,
        null::boolean AS flag_postcheck2,
        null::boolean AS done,
        null::boolean AS flag_photos,
        null::text AS meaningful_first_name,
        null::text AS meaningful_last_name,
        null::text AS meaningful_about,
        ARRAY[]::text[] AS extracted_links,
        null::text AS summary,
        null::text AS confidence,
        ARRAY[]::text[] AS urls,
        ARRAY[]::text[] AS photos
    FROM {cleaned_table_name}
    WHERE person_id = 3578 or person_id = 1537
""" #TODO: убрать Where person_id = 3578 or person_id = 1537
STATS_QUERY = f"""
    SELECT
        COUNT(*) AS total_persons,
        COUNT(CASE WHEN flag_prellm THEN 1 END) AS prellm_done,
        COUNT(CASE WHEN flag_llm THEN 1 END) AS llm_done,
        COUNT(CASE WHEN flag_perp THEN 1 END) AS perp_done,
        COUNT(CASE WHEN flag_postcheck1 THEN 1 END) AS postcheck1_done,
        COUNT(CASE WHEN flag_postcheck2 THEN 1 END) AS postcheck2_done
    FROM {result_table_name};
"""
SELECT_DONE_QUERY = f"""
    SELECT *
    FROM {result_table_name}
    WHERE done = TRUE
    ORDER BY person_id;
"""
TAKE_PENDING_TASK_QUERY = """
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
# and task_type like 'llm' 'perp' 'postcheck1' 'postcheck2'
