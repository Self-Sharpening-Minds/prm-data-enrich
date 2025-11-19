from typing import Any

from config import LlmConfig
from llm.base_llm_client import BaseLLMClient, LlmResponse


class LlmClient(BaseLLMClient):
    """Клиент для обычных LLM-вызовов.
    Использует модели согласно конфигурации.
    """

    def __init__(self, config: LlmConfig | None = None) -> None:
        super().__init__(config=config)
        self.logger.debug(
            "LlmClient инициализирован",
            extra={"default_model": self.config.model["default"]}
        )

    async def ask_json(
        self,
        prompt: str,
        *,
        model: str | None = None,
        temperature: float = 0.0 # TODO: какая температура лучше для задач? = None
    ) -> dict[str, Any]:
        """Удобный метод получения JSON от модели."""
        response: LlmResponse = await self.request(
            prompt=prompt,
            model=model,
            response_format={"type": "json_object"},
            temperature=temperature,
        )

        return response.json or {}

    async def async_parse_single_to_meaningful(self, person_data: dict) -> dict:
        """
        Обрабатывает данные одного человека через LLM.
        """
        prompt = self.prompts.render("parse_chunk", chunk_json=person_data)
        return await self.ask_json(prompt, model=self.config.model["default"])

    async def async_postcheck(self, text: str) -> bool:
        """Проверка результата моделью check."""
        prompt = self.prompts.render("postcheck", text=text)
        if not prompt:
            return False

        response = await self.ask_json(prompt, model=self.config.model["check"])
        return bool(response.get("is_valid"))

    async def async_postcheck2(self, person: dict, summary: str, urls) -> bool:
        """Расширенная проверка результата моделью check."""
        pieces = [
            f"- Имя: {person.get('meaningful_first_name', '')}",
            f"- Фамилия: {person.get('meaningful_last_name', '')}",
            f"- Доп. информация: {person.get('meaningful_about', '')}",
            f"- Ссылки, названия: {person.get('extracted_links', [])}",
        ]

        prompt = self.prompts.render(
            "postcheck2",
            pieces=pieces,
            summary=summary,
            urls=urls
        )

        if not prompt:
            return False

        response = await self.ask_json(prompt, model=self.config.model["check"])
        return bool(response.get("is_valid"))
