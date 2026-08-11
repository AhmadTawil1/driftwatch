"""Cost math, kept separate from the HTTP client so it can be unit tested
with no network involved at all."""

from dataclasses import dataclass
from decimal import Decimal


@dataclass
class Price:
    """A model's price at some point in time — one row from model_prices."""

    input_per_million: Decimal
    output_per_million: Decimal


def compute_cost(input_tokens: int, output_tokens: int, price: Price) -> Decimal:
    return (
        Decimal(input_tokens) * price.input_per_million
        + Decimal(output_tokens) * price.output_per_million
    ) / Decimal(1_000_000)
