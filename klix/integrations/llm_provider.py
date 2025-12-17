from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class LLMProvider(ABC):
    """
    Abstract LLM provider interface for KlixOS.

    All language model backends (OpenAI, Gemini, local, etc.) should implement
    this contract. Higher-level modules should depend on this interface instead
    of any specific vendor SDK.
    """

    @abstractmethod
    def generate(self, prompt: str, **kwargs: Any) -> str:
        """
        Generate a response for the given prompt.

        Implementations may accept provider-specific kwargs (e.g. temperature,
        max_tokens, system prompts). The return value is a plain string.
        """
        raise NotImplementedError


class OpenAIProvider(LLMProvider):
    """
    Stub OpenAI-backed LLMProvider.

    This is intentionally *not* wired to the actual OpenAI client yet to avoid
    guessing configuration or telemetry wiring. A future blueprint can connect
    this to your existing OpenAI usage + BrainGateway logging.
    """

    def __init__(self, model: str) -> None:
        self.model = model

    def generate(self, prompt: str, **kwargs: Any) -> str:
        """
        Placeholder implementation.

        This raises for now so nothing accidentally relies on an unwired
        integration. Existing flows should keep using their current OpenAI
        calls until this contract is fully adopted.
        """
        raise NotImplementedError(
            "OpenAIProvider.generate is not implemented yet. "
            "Use existing OpenAI flows until the integration layer is wired."
        )
