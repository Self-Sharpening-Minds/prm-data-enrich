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

    async def search_info(
        self,
        person_data: dict
    ) -> dict:
        """Ищет информацию о человеке через Perplexity."""
        pieces = self._build_search_pieces(person_data)
        prompt = self.prompts.render("perp_search", pieces=pieces)
        resp = await self.request(
            prompt=prompt,
            model=self.config.model["perplexity"],
            response_format={"type": "text"},
            temperature=0.3,
            extra_body=self._build_osint_params(),
        )
        summary = (resp.text or "").strip() or None
        urls = self._extract_urls_from_response(resp.raw)
        return {"summary": summary, "urls": urls}

    @staticmethod
    def _build_osint_params() -> dict:
        """Создаёт словарь OSINT-настроек запроса для perp."""
        return {
            "top_p": 0.9,
            "presence_penalty": 0.3,
            "frequency_penalty": 0.2,
        }

    def _build_search_pieces(self, person_data: dict) -> list[str]:
        """Формирует список строк для шаблона perp_search."""
        first_name = person_data.get("meaningful_first_name", "")
        last_name = person_data.get("meaningful_last_name", "")
        about = person_data.get("meaningful_about", "")
        extracted_links = person_data.get("extracted_links", [])
        birth_date = person_data.get("birth_date", '')

        pieces = [
            f"- Имя: {first_name}",
            f"- Фамилия: {last_name}",
        ]
        if about:
            pieces.append(f"- Доп. информация: {about}")
        if birth_date:
            pieces.append(f"- Дата рождения: {birth_date}")
        if extracted_links:
            pieces.append("- Найденные ссылки: " + ", ".join(extracted_links))
        return pieces

    def _extract_urls_from_response(self, raw_completion: Any | None) -> list[str]:
        """Извлекает URL-ы из сырого ответа completion."""
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
