from typing import Any

from config import LlmConfig
from llm.base_llm_client import BaseLLMClient


class LlmClient(BaseLLMClient):
    """Конкретный клиент для общих LLM-вызовов.
    Наследуется от BaseLLMClient и использует модель по умолчанию
    из конфигурации для обработки запросов к языковым моделям.
    Attributes:
        config (LlmConfig): Конфигурация LLM клиента
        logger: Логгер для записи событий
    """

    def __init__(self, config: LlmConfig | None = None) -> None:
        """Инициализация LLM клиента.
        Args:
            config: Конфигурация LLM. Если не указана, используется по умолчанию.
        """
        super().__init__(config=config)
        self.logger.debug("LlmClient инициализирован",
                          extra={"default_model": self.config.default_model})


    async def async_ask_llm(
        self,
        prompt: str,
        response_format: str = "json_object",
        temperature: float = 0.0
    ) -> Any:
        result, _ = await self._async_request_llm(
            prompt=prompt,
            model=self.config.default_model,
            response_format=response_format,
            temperature=temperature,
        )
        return result


    async def async_parse_single_to_meaningful(self, person_data: dict) -> dict:
        """
        Обрабатывает данные одного человека через LLM.
        Args:
            person_data: Словарь с данными одного человека
        Returns:
            Словарь с обработанными данными
        """
        prompt = self._render_prompt("parse_chunk", chunk_json=person_data)
        response = await self.async_ask_llm(prompt, response_format="json_object")
        return response if isinstance(response, dict) else {}

    async def async_postcheck(self, text: str) -> bool:
        """(async) postcheck"""
        prompt = self._render_prompt("postcheck", text=text)
        if not prompt:
            return False

        response, _ = await self._async_request_llm(
            prompt=prompt,
            model=self.config.check_model,
            response_format="json_object"
        )

        return response.get("is_valid", False) if isinstance(response, dict) else False

    async def async_postcheck2(self, person: dict, summary: str, urls) -> bool:
        """(async) postcheck2"""
        pieces = [
            f"- Имя: {person.get("meaningful_first_name", "")}",
            f"- Фамилия: {person.get("meaningful_last_name", "")}",
            f"- Доп. информация: {person.get("meaningful_about", "")}",
            f"- Ссылки, названия, которые стоило проверить: {person.get("extracted_links", [])}"
        ]

        prompt = self._render_prompt("postcheck2", pieces=pieces, summary=summary, urls=urls)
        if not prompt:
            return False

        response, _ = await self._async_request_llm(
            prompt=prompt,
            model=self.config.check_model,
            response_format="json_object"
        )

        return response.get("is_valid", False) if isinstance(response, dict) else False
