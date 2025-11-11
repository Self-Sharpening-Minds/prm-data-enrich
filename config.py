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

CHUNK_SIZE = 10

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
ASYNC_WORKERS = 4

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
