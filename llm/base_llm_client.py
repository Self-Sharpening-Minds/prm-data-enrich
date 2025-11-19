import json
import logging
from typing import Any

from config import PATH_PROMPTS, LlmConfig
from jinja2 import Environment, FileSystemLoader
from openai import AsyncOpenAI, OpenAI


class BaseLLMClient:
    """Базовый класс-обёртка для вызовов LLM через OpenAI/OpenRouter.
    Отвечает за инициализацию клиента, выполнение запросов, обработку ошибок
    и безопасный разбор JSON-ответов. Не поднимает исключения наружу —
    возвращает безопасные пустые значения.
    Attributes:
        config (LlmConfig): Конфигурация LLM клиента
        client: Клиент OpenAI
        logger: Логгер для записи событий
    """

    def __init__(self, config: LlmConfig | None = None) -> None:
        """Инициализация базового LLM клиента.
        Args:
            config: Конфигурация LLM. Если не указана, используется по умолчанию.
        """
        self.config: LlmConfig = config or LlmConfig()
        self.client = OpenAI(base_url=self.config.url, api_key=self.config.key)
        self.async_client = AsyncOpenAI(base_url=self.config.url, api_key=self.config.key)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.logger.debug("Базовый LLM клиент инициализирован",
                          extra={"config": self.config}
                          )
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
            self.logger.debug("Пустое содержимое для парсинга JSON")
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

    def _request_llm(
        self,
        prompt: str,
        model: str,
        response_format: str = "text",
        temperature: float = 0.0,
        n: int = 1,
        max_tokens: int | None = None,
        extra_body: dict[str, Any] | None = None,
    ) -> tuple[Any, Any | None]:
        """Универсальный метод выполнения запроса к LLM.
        Возвращает кортеж (parsed_content_or_raw, raw_completion_object_or_None).
        - Если response_format == "json_object" — возвращается Dict (или {} при ошибке)
        - Иначе — возвращается str (или "" при ошибке)
        Args:
            prompt: Текст промпта для отправки в LLM
            model: Название модели для использования
            response_format: Формат ответа ("text" или "json_object")
            temperature: Температура для генерации (0.0 - детерминированная)
            n: Количество вариантов ответа
            max_tokens: Максимальное количество токенов в ответе
            extra_body: Дополнительные параметры для запроса
        Returns:
            Tuple[Any, Optional[Any]]: Кортеж (результат, объект completion или None)
        """
        self.logger.debug(
            "Подготовка запроса к LLM",
            extra={
                "model": model,
                "response_format": response_format,
                "temperature": temperature,
                "n": n,
                "has_max_tokens": bool(max_tokens),
                "has_extra_body": bool(extra_body),
            },
        )

        try:
            body = extra_body.copy() if extra_body else {}
            if max_tokens:
                body["max_tokens"] = max_tokens

            rf = {"type": response_format} if response_format == "json_object" else None

            self.logger.debug("Вызов OpenAI.chat.completions.create",
                    extra={"body_preview": {k: body.get(k) for k in list(body)[:5]}}
                    )
            completion = self.client.chat.completions.create(
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
            self.logger.debug("Получен сырой ответ от LLM",
                extra={"raw_text_preview": (raw_text[:500] if raw_text else None)}
                )

            if response_format == "json_object":
                parsed = self._safe_parse_json(raw_text)
                self.logger.debug("LLM вернул JSON объект",
                                 extra={"parsed_keys": list(parsed.keys())}
                                 )
                return parsed, completion

            text_result = raw_text or ""
            self.logger.debug("LLM вернул текстовый ответ",
                             extra={"length": len(text_result)}
                             )
            return text_result, completion

        except Exception as exc:
            self.logger.debug("Детали исключения при запросе к LLM", exc_info=exc)
            self.logger.error("Ошибка при вызове LLM. Возвращаем безопасное значение.",
                              exc_info=False
                              )
            if response_format == "json_object":
                return {}, None
            return "", None

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
        self.logger.debug(
            "Подготовка АСИНХРОННОГО запроса к LLM",
            extra={
                "model": model,
                "response_format": response_format,
                "temperature": temperature,
            },
        )

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
                parsed = self._safe_parse_json(raw_text)
                return parsed, completion

            return raw_text or "", completion

        except Exception as exc:
            self.logger.error("Ошибка при асинхронном вызове LLM.", exc_info=exc)
            if response_format == "json_object":
                return {}, None
            return "", None
