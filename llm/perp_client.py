from typing import Any

from config import LlmConfig
from llm.base_llm_client import BaseLLMClient


class PerplexityClient(BaseLLMClient):
    """Клиент для работы с Perplexity моделями."""

    def __init__(self, config: LlmConfig | None = None) -> None:
        """Инициализация клиента Perplexity.
        Args:
            config: Конфигурация LLM. Если не указана, используется по умолчанию.
        """
        super().__init__(config=config)
        self.logger.debug(
            "PerplexityClient инициализирован",
            extra={"perplexity_model": self.config.model.get("perplexity")}
        )

    async def async_ask_perplexity(
        self,
        prompt: str,
        model: str | None = None,
        temperature: float = 0.2,
        use_osint_preset: bool = True,
    ) -> Any:
        """(async) ask_perplexity"""
        model_to_use = model or self.config.model["perplexity"]

        # --- OSINT preset ---
        extra_body = {}
        if use_osint_preset:
            extra_body = {
                "top_p": 0.9,
                "presence_penalty": 0.3,
                "frequency_penalty": 0.2,
                "max_tokens": 3000,
            }

        resp = await self.request(
            prompt=prompt,
            model=model_to_use,
            format="text",
            temperature=temperature,
            extra=extra_body,
        )

        urls = self._extract_urls_from_response(resp.raw)
        text = resp.text or ""
        return text, urls

    async def async_search_info(
        self,
        first_name: str,
        last_name: str,
        about: str,
        extracted_links: list[str],
        birth_date: str | None = None,
    ) -> dict:
        """(async) search_info"""
        pieces = [
            f"- Имя: {first_name}",
            f"- Фамилия: {last_name}",
        ]

        if about:
            pieces.append(f"- Доп. информация: {about}")
        if birth_date:
            pieces.append(f"- Год рождения: {birth_date}")
        if extracted_links:
            pieces.append(f"- Найденные ссылки и профили, особенно обрати внимание на них: {', '.join(extracted_links)}")

        prompt = self.prompts.render("perp_search", pieces=pieces)
        text, urls = await self.async_ask_perplexity(prompt=prompt)
        summary = text.strip() or None

        return {
            "summary": summary,
            "urls": urls,
        }

    def _extract_urls_from_response(self, raw_completion: Any | None) -> list[str]:
        """Извлекает URL-ы из сырого ответа completion.
        Args:
            completion_response: Сырой объект ответа от LLM
        Returns:
            List[str]: Список извлеченных URL или пустой список при ошибке
        """
        urls = []
        if not raw_completion:
            return urls
        try:
            for choice in getattr(raw_completion, "choices", []):
                message = getattr(choice, "message", None)
                for ann in getattr(message, "annotations", []):
                    citation = getattr(ann, "url_citation", None)
                    url = getattr(citation, "url", None)
                    if url:
                        urls.append(url)
        except Exception as exc:
            self.logger.error("Ошибка извлечения URL", exc_info=exc)
            pass
        return urls
