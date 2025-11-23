import logging
import re

from config import EMOJI_PATTERN, ENRU_CHARS_PATTERN, URL_PATTERN

logger = logging.getLogger(__name__)


def normalize_empty(value: str | None) -> str | None:
    """
    Преобразует пустые строки и пробельные значения в None.

    Примеры:
        '' → None
        '   ' → None
        None → None
        'abc' → 'abc'
    """
    if value is None:
        return None
    if isinstance(value, str):
        val = value.strip()
        # if not val:
            # logger.debug("normalize_empty: строка пуста → None")
        return val or None
    logger.debug(f"normalize_empty: пропущено значение не типа str ({type(value)})")
    return value


def _clean_common(value: str, remove_non_enru: bool = False, keep_symbols: bool = False) -> str | None:
    """
    Общая функция очистки строки от эмодзи, лишних символов и пробелов.

    Args:
        value: Исходное значение.
        remove_non_enru: Удалять символы, не входящие в паттерн ENRU_CHARS_PATTERN.
        keep_symbols: Сохранять спецсимволы, если True (используется для second_name).

    Returns:
        str | None: очищенная строка или None, если строка короткая или пуста.
    """
    if not value:
        return None

    original_value = value
    value = EMOJI_PATTERN.sub('', value)

    if remove_non_enru:
        value = ENRU_CHARS_PATTERN.sub('', value)

    if not keep_symbols:
        value = re.sub(r'[|/\\\[\]{}(),*+=<>^~"]+', ' ', value)

    value = re.sub(r'[\u200b\u200c\u200d\uFEFF]', '', value)
    value = re.sub(r'\s{2,}', ' ', value).strip()

    # logger.debug(f"_clean_common: '{original_value}' → '{value}'")

    return value if len(value) >= 2 else None


def clean_name_field(value: str | None) -> str | None:
    """Очищает поле имени — убирает эмодзи, спецсимволы, //, | и т.п."""
    return _clean_common(value or "", remove_non_enru=True, keep_symbols=False)


def clean_second_name_field(value: str | None) -> str | None:
    """Очищает поле фамилии — убирает эмодзи и zero-width символы."""
    return _clean_common(value or "", remove_non_enru=False, keep_symbols=True)


def extract_links(*fields: str | None) -> list[str]:
    """
    Извлекает уникальные ссылки из переданных строковых полей.

    Args:
        *fields: Строки, в которых могут встречаться ссылки.

    Returns:
        list[str]: Список уникальных ссылок.
    """
    found_items: list[str] = []
    for field in fields:
        if field and isinstance(field, str):
            matches = URL_PATTERN.findall(field)
            if matches:
                # logger.debug(f"extract_links: найдено {len(matches)} ссылок в '{field[:30]}...'")
                found_items.extend(matches)

    unique_links = list(dict.fromkeys(found_items))
    logger.debug(f"Извлечено {len(unique_links)} уникальных ссылок")
    return unique_links


def should_move_lastname_to_about(last_name: str | None) -> bool:
    """
    Определяет, содержит ли поле фамилии признаки некорректных данных
    (например, ссылки или email), чтобы переместить его в поле "about".
    """
    if not last_name:
        return False
    result = bool(re.search(r'\.com|@|&|http|www', last_name, re.IGNORECASE))
    logger.debug(f"should_move_lastname_to_about('{last_name}') → {result}")
    return result


def clean_summary(text: str) -> str:
    """
    Удаляет числовые ссылки в квадратных скобках и лишние пробелы из текста summary.

    Пример:
        'Текст [1] с ссылками [2]' → 'Текст с ссылками'
    """
    original_text = text
    cleaned = re.sub(r'\s*\[\d+\]\s*', ' ', text)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    # logger.debug(f"clean_summary: '{original_text[:30]}...' → '{cleaned[:30]}...'")
    return cleaned


def merge_about_fields(*fields: str | None) -> str | None:
    """
    Объединяет несколько текстовых полей в одно, очищая эмодзи и дубли.

    Args:
        *fields: Строковые поля, потенциально содержащие описание.

    Returns:
        str | None: Объединённая строка, разделённая ' | ', либо None.
    """
    unique_texts: list[str] = []

    for f in fields:
        if not f:
            continue
        cleaned = _clean_common(f, keep_symbols=True)
        if cleaned and cleaned not in unique_texts:
            unique_texts.append(cleaned)

    result = ' | '.join(unique_texts) if unique_texts else None
    # logger.debug(f"merge_about_fields: результат → '{result}'")
    return result
