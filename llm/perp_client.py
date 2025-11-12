from typing import Any

from config import LlmConfig
from llm.base_llm_client import BaseLLMClient


class PerplexityClient(BaseLLMClient):
    """
    Клиент для работы с Perplexity-like моделями (search/sonar).
    Использует Perplexity как поисковую LLM.
    Attributes:
        config (LlmConfig): Конфигурация LLM клиента
        logger: Логгер для записи событий
    """

    def __init__(self, config: LlmConfig | None = None) -> None:
        """Инициализация клиента Perplexity.
        Args:
            config: Конфигурация LLM. Если не указана, используется по умолчанию.
        """
        super().__init__(config=config)
        self.logger.debug(
            "PerplexityClient инициализирован",
            extra={"perplexity_model": self.config.perplexity_model}
        )

    def ask_perplexity(
        self,
        prompt: str,
        model: str | None = None,
        response_format: str = "text",
        temperature: float = 0.2,
        max_tokens: int | None = None,
    ) -> Any:
        """Универсальный метод для обращения к Perplexity через OpenRouter.
        Args:
            prompt: Текст промпта для отправки в модель
            model: Название модели. Если не указана, используется по умолчанию
            response_format: Формат ответа ("text" или "json_object")
            temperature: Температура для генерации
            max_tokens: Максимальное количество токенов в ответе
        Returns:
            Any: Если response_format == "json_object" - словарь (или {} при ошибке),
                 иначе - кортеж (text, urls) где text - строка, urls - список URL
        """
        model_to_use = model or self.config.perplexity_model
        self.logger.debug("Вызов ask_perplexity",
            extra={
                "model": model_to_use,
                "response_format": response_format
            }
        )

        result, completion = self._request_llm(
            prompt=prompt,
            model=model_to_use,
            response_format=response_format,
            temperature=temperature,
        )

        if response_format == "json_object":
            if isinstance(result, dict):
                return result
            self.logger.warning(
                "Ожидался словарь для response_format json_object, "
                "но получен другой тип; возвращаем {}."
            )
            return {}

        urls = self._extract_urls_from_response(completion)
        text_result = result if isinstance(result, str) else ""
        return text_result, urls

    def ask_sonar(self, prompt: str, response_format: str = "text",
                  temperature: float = 0.1) -> Any:
        """Удобный метод для модели Sonar.
        Использует модель Perplexity по умолчанию для упрощенного вызова.
        Args:
            prompt: Текст промпта для отправки в модель
            response_format: Формат ответа ("text" или "json_object")
            temperature: Температура для генерации
        Returns:
            Any: Результат вызова ask_perplexity
        """
        return self.ask_perplexity(
            prompt=prompt,
            model=self.config.perplexity_model,
            response_format=response_format,
            temperature=temperature
        )

    def search_info(
        self,
        first_name: str,
        last_name: str,
        about: str,
        birth_date: str | None = None,
        personal_channel_name: str | None = None,
        personal_channel_about: str | None = None
    ) -> dict:
        """Генерирует краткую аналитическую справку о человеке.
        Формирует промпт на основе предоставленных данных и вызывает
        Perplexity для поиска дополнительной информации в интернете.
        Args:
            first_name: Имя человека
            last_name: Фамилия человека
            additional_info: Дополнительная информация о человеке
            birth_date: Дата рождения (опционально)
            personal_channel_name: Название личного канала (опционально)
            personal_channel_about: Описание личного канала (опционально)
            response_format: Формат ответа ("text" или "json_object")
            temperature: Температура для генерации
        Returns:
            Any: Результат вызова ask_perplexity
        """
        pieces = [
            f"- Имя: {first_name}",
            f"- Фамилия: {last_name}",
            f"- Доп. информация: {about}",
        ]
        if birth_date:
            pieces.append(f"- Дата рождения: {birth_date}")
        if personal_channel_name:
            pieces.append(f"- Название канала: {personal_channel_name}")
        if personal_channel_about:
            pieces.append(f"- Описание канала: {personal_channel_about}")

        prompt = self._render_prompt(
            "perp_search",
            pieces=pieces
        )

        self.logger.debug(
            "Промпт для search_info подготовлен",
            extra={"prompt_preview": (prompt[:200] + "...")
                   if len(prompt) > 200 else prompt}
        )

        text, urls = self.ask_perplexity(prompt=prompt)

        summary = text.strip() or None
        person_found = bool(summary)  # TODO можно что-то лучше придумать
        confidence = self._estimate_confidence(summary, len(urls))

        return {
            "summary": summary,
            "urls": urls,
            "person_found": person_found,
            "confidence": confidence,
        }

    def _estimate_confidence(self, summary: str | None, sources: int) -> str:
        """Простейшая эвристика уверенности."""
        markers_l = [
            "поиск по запросу",
            "найти не удалось", "данных не найдено"
        ]
        if not summary or not (sources) or any(m in summary.lower() for m in markers_l):
            return "low"

        markers_m = [
            "по всей видимости", "вероятно",
            "предположительно"
        ]
        return "medium" if any(m in summary.lower() for m in markers_m) else "high"

    def _extract_urls_from_response(self, completion_response: Any | None) -> list[str]:
        """Извлекает URL-ы из сырого ответа completion.
        Осторожно обходит структуру ответа для извлечения аннотаций
        и цитат с URL без выбрасывания исключений.
        Args:
            completion_response: Сырой объект ответа от LLM
        Returns:
            List[str]: Список извлеченных URL или пустой список при ошибке
        """
        if completion_response is None:
            self.logger.debug("Не предоставлен completion_response для извлечения URL")
            return []

        urls: list[str] = []
        try:
            choices = getattr(completion_response, "choices", None) or []
            if not choices:
                self.logger.debug("Нет choices в completion_response при извлечении URL")
            for ch in choices:
                message = getattr(ch, "message", None)
                if not message:
                    continue
                annotations = getattr(message, "annotations", None) or []
                for ann in annotations:
                    url_citation = getattr(ann, "url_citation", None)
                    if url_citation:
                        url = getattr(url_citation, "url", None)
                        if url:
                            urls.append(url)
            self.logger.debug("URL извлечены", extra={"urls_count": len(urls)})
            return urls
        except Exception as exc:
            self.logger.error("Ошибка при извлечении URL из completion response",
                              exc_info=exc
                              )
            return []

    # --- Асинхронные методы ---
    async def async_ask_perplexity(
        self,
        prompt: str,
        model: str | None = None,
        response_format: str = "text",
        temperature: float = 0.2,
    ) -> Any:
        """(async) ask_perplexity"""
        model_to_use = model or self.config.perplexity_model

        result, completion = await self._async_request_llm(
            prompt=prompt,
            model=model_to_use,
            response_format=response_format,
            temperature=temperature,
        )

        if response_format == "json_object":
            return result if isinstance(result, dict) else {}

        urls = self._extract_urls_from_response(completion)
        text_result = result if isinstance(result, str) else ""
        return text_result, urls

    async def async_search_info(
        self,
        first_name: str,
        last_name: str,
        about: str,
        extracted_links: list[str],
        birth_date: str | None = None,
        personal_channel_name: str | None = None,
        personal_channel_about: str | None = None
    ) -> dict:
        """(async) search_info"""
        pieces = [
            f"- Имя: {first_name}",
            f"- Фамилия: {last_name}",
        ]
        if about:
            pieces.append(f"- Доп. информация: {about}")
        if extracted_links:
            links_str = ", ".join(extracted_links)
            pieces.append(f"- Найденные ссылки и профили, особенно обрати внимание на них: {links_str}")

        prompt = self._render_prompt(
            "perp_search",
            pieces=pieces
        )

        if not prompt:
            return {"summary": None, "urls": [], "person_found": False, "confidence": "low"}

        text, urls = await self.async_ask_perplexity(prompt=prompt)

        summary = text.strip() or None
        person_found = bool(summary)
        confidence = self._estimate_confidence(summary, len(urls))

        return {
            "summary": summary,
            "urls": urls,
            "person_found": person_found,
            "confidence": confidence,
        }
