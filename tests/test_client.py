"""Integration tests against the mock provider server (real TCP, not a
mocked transport object) — this is where retry and failure recording get
proven, per the plan: provoking these against a real provider is
unreliable and expensive.
"""

import asyncio
from decimal import Decimal

from mock_provider import MockProviderServer
from watchdog.providers.client import ProviderClient
from watchdog.providers.pricing import Price, compute_cost

PRICE = Price(input_per_million=Decimal("0.15"), output_per_million=Decimal("0.60"))


async def test_success_populates_every_field():
    with MockProviderServer(mode="normal") as server:
        async with ProviderClient("openai", server.base_url, "fake-key", concurrency_limit=5) as client:
            result = await client.call("gpt-4o-mini", "hello", PRICE)

    assert result.error is None
    assert result.output_text == "mock response"
    assert result.input_tokens == 10
    assert result.output_tokens == 5
    assert result.provider_model_version == "gpt-4o-mini-mock"
    assert result.latency_ms >= 0
    assert result.cost_usd == compute_cost(10, 5, PRICE)


async def test_two_429s_then_success_is_one_result_no_duplicate():
    with MockProviderServer(mode="rate_limited_then_success", fail_times=2) as server:
        async with ProviderClient(
            "openai", server.base_url, "fake-key", concurrency_limit=5, base_delay_s=0.01
        ) as client:
            result = await client.call("gpt-4o-mini", "hello", PRICE)

    assert result.error is None
    assert result.output_text == "mock response"
    assert server.call_count == 3  # two failures + the one that succeeded


async def test_permanent_timeout_is_a_recorded_failure_not_a_zero_score():
    with MockProviderServer(mode="timeout", sleep_s=1.0) as server:
        async with ProviderClient(
            "openai",
            server.base_url,
            "fake-key",
            concurrency_limit=5,
            base_delay_s=0.01,
            max_retries=1,
            timeout_s=0.2,
        ) as client:
            result = await client.call("gpt-4o-mini", "hello", PRICE)

    # A failure is recorded — output/tokens/cost stay None rather than
    # being coerced into a score of 0 (F14).
    assert result.error is not None
    assert result.output_text is None
    assert result.cost_usd is None


async def test_malformed_json_is_a_recorded_failure():
    with MockProviderServer(mode="malformed_json") as server:
        async with ProviderClient(
            "openai", server.base_url, "fake-key", concurrency_limit=5, base_delay_s=0.01
        ) as client:
            result = await client.call("gpt-4o-mini", "hello", PRICE)

    assert result.error is not None
    assert "malformed response" in result.error
    assert result.output_text is None


async def test_truncated_response_is_a_recorded_failure():
    with MockProviderServer(mode="truncated") as server:
        async with ProviderClient(
            "openai",
            server.base_url,
            "fake-key",
            concurrency_limit=5,
            base_delay_s=0.01,
            max_retries=1,
        ) as client:
            result = await client.call("gpt-4o-mini", "hello", PRICE)

    assert result.error is not None
    assert result.output_text is None


def test_cost_accounting_matches_hand_computed_values():
    price = Price(input_per_million=Decimal("2.50"), output_per_million=Decimal("10.00"))

    # Round numbers, easy to check by hand:
    # 1,000,000 input tokens @ $2.50/M = $2.50 exactly.
    #   500,000 output tokens @ $10.00/M = $5.00 exactly.
    cost = compute_cost(input_tokens=1_000_000, output_tokens=500_000, price=price)
    assert cost == Decimal("7.50")

    # Ugly numbers, checked against the same formula computed independently.
    cost_small = compute_cost(input_tokens=37, output_tokens=91, price=price)
    expected = (
        Decimal(37) * Decimal("2.50") / Decimal(1_000_000)
        + Decimal(91) * Decimal("10.00") / Decimal(1_000_000)
    )
    assert cost_small == expected


async def test_semaphore_caps_in_flight_requests():
    with MockProviderServer(mode="slow_normal", sleep_s=0.3) as server:
        async with ProviderClient(
            "openai", server.base_url, "fake-key", concurrency_limit=3
        ) as client:
            await asyncio.gather(*(client.call("gpt-4o-mini", "hello", PRICE) for _ in range(8)))

    assert server.max_in_flight <= 3
