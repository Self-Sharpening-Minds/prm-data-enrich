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

    async def async_ask_perplexity(
        self,
        prompt: str,
        model: str | None = None,
        response_format: str = "text",
        temperature: float = 0.2,
        use_osint_preset: bool = True,
    ) -> Any:
        """(async) ask_perplexity"""
        model_to_use = model or self.config.perplexity_model

        # --- OSINT preset ---
        extra_body = {}
        if use_osint_preset:
            extra_body.update({
                "top_p": 0.9,
                "presence_penalty": 0.3,
                "frequency_penalty": 0.2,
                "max_tokens": 3000,
            })

        result, completion = await self._async_request_llm(
            prompt=prompt,
            model=model_to_use,
            response_format=response_format,
            temperature=temperature,
            extra_body=extra_body,
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
            pieces.append(f"- Найденные ссылки и профили, особенно обрати внимание на них: {', '.join(extracted_links)}")

        prompt = self._render_prompt("perp_search", pieces=pieces)
        if not prompt:
            return {"summary": None, "urls": [], "person_found": False, "confidence": "low"}

        text, urls = await self.async_ask_perplexity(prompt=prompt)
        summary = text.strip() or None

        return {
            "summary": summary,
            "urls": urls,
            "person_found": bool(summary),
            "confidence": self._estimate_confidence(summary, len(urls)),
        }


    def _estimate_confidence(self, summary: str | None, sources: int) -> str:
        """Простейшая эвристика уверенности."""
        if not summary or not sources:
            return "low"
        lower = summary.lower()
        if any(x in lower for x in ["найти не удалось", "данных не найдено"]):
            return "low"
        if any(x in lower for x in ["вероятно", "предположительно"]):
            return "medium"
        return "high"

    def _extract_urls_from_response(self, completion_response: Any | None) -> list[str]:
        """Извлекает URL-ы из сырого ответа completion.
        Осторожно обходит структуру ответа для извлечения аннотаций
        и цитат с URL без выбрасывания исключений.
        Args:
            completion_response: Сырой объект ответа от LLM
        Returns:
            List[str]: Список извлеченных URL или пустой список при ошибке
        """
        urls = []
        if not completion_response:
            return urls
        try:
            for ch in getattr(completion_response, "choices", []):
                msg = getattr(ch, "message", None)
                for ann in getattr(msg, "annotations", []):
                    url = getattr(getattr(ann, "url_citation", None), "url", None)
                    if url:
                        urls.append(url)
        except Exception as exc:
            self.logger.error("Ошибка при извлечении URL из completion response", exc_info=exc)
            pass
        return urls
