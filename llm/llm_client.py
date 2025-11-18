import json
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

    def ask_llm(self, prompt: str,
                response_format: str = "json_object",
                temperature: float = 0.0
        ) -> Any:
        """Универсальный метод для обращения к LLM.
        Выполняет запрос к языковой модели с указанными параметрами
        и возвращает результат в заданном формате.
        Args:
            prompt: Текст промпта для отправки в LLM
            response_format: Формат ответа ("json_object" или "text")
            temperature: Температура для генерации (0.0 - детерминированная)
        Returns:
            Any: Словарь если response_format == "json_object", иначе строка.
                 В случае ошибки возвращает {} или "".
        """
        result, _raw = self._request_llm(
            prompt=prompt,
            model=self.config.default_model,
            response_format=response_format,
            temperature=temperature,
        )
        return result

    def parse_chunk_to_meaningful(self, chunk: dict[str, str]) -> dict[str, Any]:
        """Обрабатывает пачку записей для извлечения имен и информации.
        Преобразует chunk данных в JSON, отправляет в LLM для анализа
        и возвращает структурированные данные об именах и информации.
        Args:
            chunk: Словарь с данными для обработки
        Returns:
            Dict[str, Any]: Словарь с обработанными данными или пустой словарь при ошибке
        """
        try:
            chunk_json = json.dumps(chunk, ensure_ascii=False)
        except Exception as exc:
            self.logger.error("Chunk не может быть сериализован в JSON; "
                            "возвращаем пустой результат.", exc_info=exc
                            )
            return {}

        prompt = self._render_prompt(
            "parse_chunk",
            chunk_size=len(chunk),
            chunk_json=chunk_json
        )

        self.logger.debug("Вызов ask_llm для parse_names_and_about_chunk",
                          extra={"chunk_size": len(chunk)}
                          )
        response = self.ask_llm(prompt, response_format="json_object")
        if not isinstance(response, dict):
            self.logger.warning("Ожидался словарь, но получен другой тип; возвращаем {}.")
            return {}
        return response

    def postcheck(self, text) -> bool:
        """
        Проверяет, является ли текст содержательным описанием человека или заглушкой.
        Возвращает словарь с результатом классификации.
        Args:
            chunk: Текст, который надо проверить
        """
        prompt = self._render_prompt(
            "postcheck",
            text=text
        )

        response, _raw = self._request_llm(
            prompt=prompt,
            model=self.config.check_model,
            response_format="json_object"
        )

        if not isinstance(response, dict) or "is_valid" not in response:
            self.logger.warning("Некорректный формат ответа от нейросети; возвращаем False по умолчанию.")
            return False

        return response.get("is_valid", False)

    # --- Асинхронные методы ---
    async def async_ask_llm(self, prompt: str,
                response_format: str = "json_object",
                temperature: float = 0.3
        ) -> Any:
        """(async) async_ask_llm."""
        result, _raw = await self._async_request_llm(
            prompt=prompt,
            model=self.config.default_model,
            response_format=response_format,
            temperature=temperature,
        )
        return result

    async def async_parse_chunk_to_meaningful(self, chunk: dict[str, str]) -> dict[str, Any]:
        """(async) parse_chunk_to_meaningful"""
        try:
            chunk_json = json.dumps(chunk, ensure_ascii=False)
        except Exception as exc:
            self.logger.error("Chunk не может быть сериализован в JSON; "
                            "возвращаем пустой результат.", exc_info=exc
            )
            return {}

        prompt = self._render_prompt(
            "parse_chunk",
            chunk_size=len(chunk),
            chunk_json=chunk_json
        )

        self.logger.debug("Асинхронный вызов async_ask_llm для async_parse_chunk_to_meaningful",
                          extra={"chunk_size": len(chunk)}
        )

        response = await self.async_ask_llm(prompt, response_format="json_object")
        if not isinstance(response, dict):
            self.logger.warning("Ожидался словарь, но получен другой тип; возвращаем {}.")
            return {}
        return response

    async def async_parse_single_to_meaningful(self, person_data: dict) -> dict:
        """
        Обрабатывает данные одного человека через LLM.
        Args:
            person_data: Словарь с данными одного человека
        Returns:
            Словарь с обработанными данными
        """
        prompt = self._render_prompt(
            "parse_chunk",
            chunk_json=person_data
        )
        response = await self.async_ask_llm(prompt, response_format="json_object")
        if not isinstance(response, dict):
            self.logger.warning("Ожидался словарь, но получен другой тип; возвращаем {}.")
            return {}
        return response

    async def async_postcheck(self, text: str) -> bool:
        """(async) postcheck"""
        prompt = self._render_prompt("postcheck", text=text)

        if not prompt:
            return False

        response, _raw = await self._async_request_llm(
            prompt=prompt,
            model=self.config.check_model,
            response_format="json_object"
        )

        if not isinstance(response, dict) or "is_valid" not in response:
            self.logger.warning("Некорректный формат ответа; возвращаем False.")
            return False

        return response.get("is_valid", False)

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

        response, _raw = await self._async_request_llm(
            prompt=prompt,
            model=self.config.check_model,
            response_format="json_object"
        )

        if not isinstance(response, dict) or "is_valid" not in response:
            self.logger.warning("Некорректный формат ответа; возвращаем False.")
            return False

        return response.get("is_valid", False)
