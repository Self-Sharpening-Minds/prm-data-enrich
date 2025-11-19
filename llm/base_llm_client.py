import json
import logging
from typing import Any

from config import PATH_PROMPTS, LlmConfig
from jinja2 import Environment, FileSystemLoader
from openai import AsyncOpenAI, OpenAI


class BaseLLMClient:
    """Базовый async-клиент для OpenAI/OpenRouter."""

    def __init__(self, config: LlmConfig | None = None) -> None:
        """Инициализация базового LLM клиента.
        Args:
            config: Конфигурация LLM. Если не указана, используется по умолчанию.
        """
        self.config: LlmConfig = config or LlmConfig()
        self.async_client = AsyncOpenAI(base_url=self.config.url, api_key=self.config.key)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        # self.logger.debug("Базовый LLM клиент инициализирован",
        #                   extra={"config": self.config}
        #                   )

        self.jinja_env = Environment(
            loader=FileSystemLoader(PATH_PROMPTS),
            autoescape=True
        )

    def _safe_parse_json(self, raw: str | None) -> dict[str, Any]:
        """Безопасно парсит JSON строку.
        Пытается распарсить JSON; в случае ошибки — логирует и возвращает пустой словарь.
        Args:
            raw: Сырая JSON строка для парсинга
        Returns:
            Dict[str, Any]: Распарсенный JSON как словарь или пустой словарь при ошибке
        """
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
            self.logger.debug("JSON успешно распарсен",
                              extra={"parsed_type": type(parsed).__name__}
                              )
            return parsed if isinstance(parsed, dict) else {"result": parsed}
        except json.JSONDecodeError as exc:
            self.logger.warning("Невалидный JSON от LLM",
                                exc_info=exc, extra={"raw": raw}
                                )
            return {}
        except Exception as exc:
            self.logger.error("Неожиданная ошибка при парсинге JSON",
                              exc_info=exc, extra={"raw": raw}
                              )
            return {}

    def _render_prompt(self, template_name: str, **kwargs) -> str:
        """Загружает и рендерит шаблон промпта с переданными переменными."""
        try:
            template = self.jinja_env.get_template(f"{template_name}.jinja2")
            return template.render(**kwargs)
        except Exception as e:
            self.logger.error(f"Ошибка рендеринга шаблона {template_name}: {e}")
            return ""

    async def _async_request_llm(
        self,
        prompt: str,
        model: str,
        response_format: str = "text",
        temperature: float = 0.0,
        n: int = 1,
        max_tokens: int | None = None,
        extra_body: dict[str, Any] | None = None,
    ) -> tuple[Any, Any | None]:
        """
        (async) _request_llm
        """
        # self.logger.debug(
        #     "Подготовка АСИНХРОННОГО запроса к LLM",
        #     extra={
        #         "model": model,
        #         "response_format": response_format,
        #         "temperature": temperature,
        #     },
        # )

        try:
            body = extra_body.copy() if extra_body else {}
            if max_tokens:
                body["max_tokens"] = max_tokens

            rf = {"type": response_format} if response_format == "json_object" else None #TODO:

            completion = await self.async_client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                response_format=rf,
                temperature=temperature,
                n=n,
                extra_body=body or None,
            )

            content = getattr(getattr(completion, "choices", [None])[0], "message", None)
            raw_text: str | None = None
            if content is not None:
                raw_text = getattr(content, "content", None)

            self.logger.debug("Получен сырой ответ от LLM (async)",
                extra={"raw_text_preview": (raw_text[:500] if raw_text else None)}
            )

            if response_format == "json_object":
                return self._safe_parse_json(raw_text), completion

            return raw_text or "", completion

        except Exception as exc:
            self.logger.error("Ошибка при асинхронном вызове LLM.", exc_info=exc)
            if response_format == "json_object":
                return {}, None
            return "", None
