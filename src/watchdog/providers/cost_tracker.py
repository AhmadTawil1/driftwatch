"""A running total for one nightly run, with a hard ceiling.

Deliberately does *not* stop dispatch by itself — it just tracks the
total and exposes `.exceeded`. The executor (day 3) is what actually
checks this after writing each result and decides to stop; that keeps
the decision to abort next to the code that writes rows, where the plan
says it belongs ("check running cost after every write").
"""

import asyncio
from decimal import Decimal


class CostTracker:
    def __init__(self, cap_usd: Decimal | None) -> None:
        self.cap_usd = cap_usd
        self._total = Decimal("0")
        self._lock = asyncio.Lock()

    @property
    def total_usd(self) -> Decimal:
        return self._total

    @property
    def exceeded(self) -> bool:
        return self.cap_usd is not None and self._total >= self.cap_usd

    async def add(self, amount: Decimal) -> Decimal:
        async with self._lock:
            self._total += amount
            return self._total
