"""One instance of ProviderClient per provider (OpenAI, Anthropic, ...).
Talks OpenAI-compatible chat completions — the plan's assumption is that
every provider exposes that shape, so switching provider is a base_url
change, not a rewrite.

Retries happen here (429 / 5xx / network errors); scoring never happens
here. This file only ever produces a CallResult — it doesn't touch the
database and doesn't decide whether a result is "good."
"""

import asyncio
import random
import time
from dataclasses import dataclass

import httpx

from watchdog.providers.pricing import Price, compute_cost

# 429 = rate limited, 5xx = the provider's problem — both are worth
# retrying. Other 4xx (400 bad request, 401 bad key, 404 unknown model)
# will fail identically every time, so retrying them just burns the
# retry budget for no reason.
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


@dataclass
class CallResult:
    output_text: str | None
    latency_ms: int
    input_tokens: int | None
    output_tokens: int | None
    cost_usd: object | None  # Decimal | None — see pricing.py
    provider_model_version: str | None
    error: str | None


class ProviderClient:
    def __init__(
        self,
        provider_name: str,
        base_url: str,
        api_key: str,
        concurrency_limit: int,
        max_retries: int = 5,
        base_delay_s: float = 1.0,
        timeout_s: float = 60.0,
    ) -> None:
        self.provider_name = provider_name
        self.max_retries = max_retries
        self.base_delay_s = base_delay_s
        self._semaphore = asyncio.Semaphore(concurrency_limit)
        self._http = httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout_s,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "ProviderClient":
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.aclose()

    async def call(self, model: str, prompt: str, price: Price) -> CallResult:
        """One (model, prompt) call, retried under this provider's
        semaphore. Temperature 0 — the plan treats this as "near
        determinism," not a guarantee, which is why the suite repeats
        each task rather than trusting a single call."""
        async with self._semaphore:
            start = time.monotonic()
            last_error: str | None = None

            for attempt in range(self.max_retries + 1):
                try:
                    response = await self._http.post(
                        "/chat/completions",
                        json={
                            "model": model,
                            "messages": [{"role": "user", "content": prompt}],
                            "temperature": 0,
                        },
                    )
                except (httpx.TimeoutException, httpx.TransportError) as exc:
                    last_error = f"{type(exc).__name__}: {exc}"
                    if attempt >= self.max_retries:
                        break
                    await self._sleep_backoff(attempt)
                    continue

                if response.status_code in RETRYABLE_STATUS_CODES:
                    last_error = f"HTTP {response.status_code}: {response.text[:500]}"
                    if attempt >= self.max_retries:
                        break
                    await self._sleep_backoff(attempt)
                    continue

                latency_ms = int((time.monotonic() - start) * 1000)

                if response.status_code >= 400:
                    # Non-retryable — this request will never succeed no
                    # matter how many times we ask.
                    return CallResult(
                        output_text=None,
                        latency_ms=latency_ms,
                        input_tokens=None,
                        output_tokens=None,
                        cost_usd=None,
                        provider_model_version=None,
                        error=f"HTTP {response.status_code}: {response.text[:500]}",
                    )

                return self._parse_success(response, latency_ms, price)

            # Retries exhausted. error is set, everything else stays
            # None — never counted as a zero score (F14).
            latency_ms = int((time.monotonic() - start) * 1000)
            return CallResult(
                output_text=None,
                latency_ms=latency_ms,
                input_tokens=None,
                output_tokens=None,
                cost_usd=None,
                provider_model_version=None,
                error=last_error or "exhausted retries with no response",
            )

    def _parse_success(
        self, response: httpx.Response, latency_ms: int, price: Price
    ) -> CallResult:
        try:
            data = response.json()
            output_text = data["choices"][0]["message"]["content"]
            usage = data.get("usage") or {}
            input_tokens = usage.get("prompt_tokens")
            output_tokens = usage.get("completion_tokens")
            # The model string the provider actually served, which can
            # differ from what we requested — this is what later reveals
            # a provider swapping weights under a stable alias.
            provider_model_version = data.get("model")
        except (ValueError, KeyError, IndexError) as exc:
            return CallResult(
                output_text=None,
                latency_ms=latency_ms,
                input_tokens=None,
                output_tokens=None,
                cost_usd=None,
                provider_model_version=None,
                error=f"malformed response: {type(exc).__name__}: {exc}",
            )

        cost_usd = None
        if input_tokens is not None and output_tokens is not None:
            cost_usd = compute_cost(input_tokens, output_tokens, price)

        return CallResult(
            output_text=output_text,
            latency_ms=latency_ms,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=cost_usd,
            provider_model_version=provider_model_version,
            error=None,
        )

    async def _sleep_backoff(self, attempt: int) -> None:
        delay = self.base_delay_s * (2**attempt)
        jitter = random.uniform(0, delay * 0.5)
        await asyncio.sleep(delay + jitter)
