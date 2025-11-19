import json
import logging
from typing import Any

from config import PATH_PROMPTS, LlmConfig, LlmResponse
from jinja2 import Environment, FileSystemLoader
from openai import AsyncOpenAI


class PromptRenderer:
    def __init__(self, path: str):
        self.env = Environment(
            loader=FileSystemLoader(path),
            autoescape=True
        )

    def render(self, template: str, **kwargs) -> str:
        try:
            t = self.env.get_template(f"{template}.jinja2")
            return t.render(**kwargs)
        except Exception as e:
            raise RuntimeError(f"Ошибка рендеринга шаблона '{template}': {e}") from e


class BaseLLMClient:
    """Базовый async-клиент для OpenAI/OpenRouter."""

    def __init__(self, config: LlmConfig | None = None):
        self.config = config or LlmConfig()
        self.logger = logging.getLogger(__name__)

        self._client: AsyncOpenAI | None = None
        self.prompts = PromptRenderer(PATH_PROMPTS)

    @property
    def client(self) -> AsyncOpenAI:
        if self._client is None:
            self._client = AsyncOpenAI(
                base_url=self.config.url,
                api_key=self.config.key,
            )
        return self._client

    async def request(
        self,
        prompt: str,
        model: str | None = None,
        *,
        max_tokens: int | None = None,
        n: int = 1,
        response_format: dict | None = None,
        temperature: float | None = None,
        extra_body: dict | None = None,
    ) -> LlmResponse:
        model = model or self.config.model["default"]
        try:
            completion = await self.client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model=model,
                max_tokens=max_tokens,
                n=n,
                response_format=response_format,
                temperature=temperature,
                extra_body=extra_body,
            )

            msg = completion.choices[0].message
            raw_text = msg.content if msg else None
            data_json = self._parse_json(raw_text) if response_format == {"type": "json_object"} else None
            return LlmResponse(
                text=raw_text,
                json=data_json,
                raw=completion,
                usage=getattr(completion, "usage", None),
            )

        except Exception as exc:
            self.logger.error("Ошибка LLM запроса", exc_info=exc)
            return LlmResponse(
                text=None,
                json={} if response_format == {"type": "json_object"} else None,
                raw=None,
                usage=None,
            )

    def _parse_json(self, raw: str | None) -> dict[str, Any]:
        if not raw: return {}
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {"result": parsed}
        except Exception as exc:
            self.logger.warning("Ошибка JSON", exc_info=exc, extra={"raw": raw})
            return {}
