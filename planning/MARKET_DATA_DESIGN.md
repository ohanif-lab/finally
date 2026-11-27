# Market Data Backend — Detailed Design (Revision 2)

Implementation-ready design for the FinAlly market data subsystem: the unified
interface, the shared price cache, the GBM simulator, the Massive (Polygon.io)
REST client, the SSE endpoint, and how the rest of the backend plugs into it.

Everything under "Module design" lives in `backend/app/market/`. The integration
sections (FastAPI app, ticker tracking, consumers) describe code owned by the
backend agent that *uses* the market package.

> **Relationship to earlier docs.** `planning/archive/MARKET_DATA_DESIGN.md` is the
> original design, and `planning/MARKET_DATA_SUMMARY.md` describes what was built
> from it (73 passing tests). This revision is written against the **code as it
> exists today** in `backend/app/market/`. It keeps the architecture and public API
> and specifies a set of corrections and additions (§1.2). Each one is marked
> **[R2]** where it appears. Where this document and the archive disagree, this
> document wins.

---

## Table of Contents

1. [Overview](#1-overview)
2. [File Layout](#2-file-layout)
3. [Data Model — `models.py`](#3-data-model--modelspy)
4. [Price Cache — `cache.py`](#4-price-cache--cachepy)
5. [Unified Interface — `interface.py`](#5-unified-interface--interfacepy)
6. [Ticker Normalization — `tickers.py`](#6-ticker-normalization--tickerspy)
7. [Seed Prices & Parameters — `seed_prices.py`](#7-seed-prices--parameters--seed_pricespy)
8. [GBM Simulator — `simulator.py`](#8-gbm-simulator--simulatorpy)
9. [Massive API Client — `massive_client.py`](#9-massive-api-client--massive_clientpy)
10. [Factory & Configuration — `factory.py`](#10-factory--configuration--factorypy)
11. [SSE Streaming — `stream.py`](#11-sse-streaming--streampy)
12. [Package Exports — `__init__.py`](#12-package-exports--__init__py)
13. [FastAPI Integration](#13-fastapi-integration)
14. [Ticker Tracking: Watchlist ∪ Positions](#14-ticker-tracking-watchlist--positions)
15. [Downstream Consumers](#15-downstream-consumers)
16. [Frontend Contract](#16-frontend-contract)
17. [Testing](#17-testing)
18. [Error Handling & Edge Cases](#18-error-handling--edge-cases)
19. [Configuration Reference](#19-configuration-reference)
20. [Implementation Checklist](#20-implementation-checklist)

---

## 1. Overview

### 1.1 Architecture

```
                 ┌──────────────────────────────┐
                 │  create_market_data_source() │  reads MASSIVE_API_KEY
                 └──────────────┬───────────────┘
                                │ returns one of
             ┌──────────────────┴───────────────────┐
             ▼                                      ▼
┌───────────────────────────┐        ┌───────────────────────────────┐
│ SimulatorDataSource       │        │ MassiveDataSource             │
│  GBMSimulator.step()      │        │  RESTClient.get_snapshot_all()│
│  every 0.5s (async task)  │        │  every 15s (thread + task)    │
└─────────────┬─────────────┘        └───────────────┬───────────────┘
              │      both implement MarketDataSource │
              └──────────────────┬───────────────────┘
                                 │ write
                                 ▼
                 ┌───────────────────────────────┐
                 │ PriceCache (thread-safe)      │
                 │ ticker → PriceUpdate          │
                 │ version counter               │
                 └───┬───────────┬───────────┬───┘
                     │ read      │ read      │ read
                     ▼           ▼           ▼
           SSE /api/stream   Trades &     LLM context,
           /prices           portfolio    watchlist GET,
           (0.5s cadence)    valuation    snapshots
```

Design principles:

- **Strategy pattern.** One abstract `MarketDataSource`, two implementations.
  Nothing downstream knows which one is running.
- **Push into a cache, read from the cache.** Sources write on their own schedule.
  Consumers only ever read `PriceCache`. The 0.5s simulator and the 15s Massive
  poller look identical to the SSE layer.
- **One value type.** `PriceUpdate` is the only data structure that leaves the
  package.
- **No I/O on the hot path for consumers.** A trade never waits on a network call
  to get its price. It reads the cache.

### 1.2 Revision 2 changes at a glance

| # | Area | Problem in current code | R2 change |
|---|------|-------------------------|-----------|
| 1 | Massive | `snap.last_trade.timestamp` does not exist on the `massive` `LastTrade` model (the fields are `sip_timestamp`/`participant_timestamp`, in **nanoseconds**). Every real snapshot raises `AttributeError`, gets skipped, and the cache stays empty. The tests miss this because they use `MagicMock`. | Parse the real model fields. Fall back from `last_trade.price` to `day.close` and then `prev_day.close`. Convert timestamp units robustly. Add tests built from real `TickerSnapshot.from_dict(...)` payloads. |
| 2 | Models | The watchlist needs **daily change %** (PLAN §10), but `change_percent` is tick-to-tick. | Add `reference_price` (previous close for Massive, session open for the simulator) plus `day_change` and `day_change_percent`. |
| 3 | Tickers | Massive upper-cases tickers and the simulator does not, so `"aapl"` and `"AAPL"` become two simulated tickers. | A single `normalize_ticker()` used by both sources and by the API layer. |
| 4 | Tracking | The archive design removes a ticker from the source on watchlist delete even if the user still holds shares, which breaks valuation. | Track **watchlist ∪ open positions**. Removal goes through one helper (§14). |
| 5 | Cache | `version` is read without the lock. A batch of N updates bumps the version N times. | Lock-protected `version`. `update_many()` bumps the version once per tick. |
| 6 | SSE | A module-level `router` gets the route registered twice if the factory is called twice (tests). There is no keepalive while Massive is idle for 15s. `CancelledError` is swallowed. | A fresh `APIRouter` per call, a `: keepalive` comment every 15s, and `CancelledError` re-raised. |
| 7 | Massive | A failing key or a 429 is retried at full rate forever. The ticker list is shared with a worker thread while being mutated. | Exponential backoff (capped). Pass the worker thread an immutable copy of the ticker list. |
| 8 | Simulator | Unknown tickers get `random.uniform(50, 300)`, so the price jumps if a ticker is removed and re-added or the app restarts. | A deterministic seed price derived from the ticker symbol. |
| 9 | Config | The Massive poll interval is hard-coded. | `MASSIVE_POLL_INTERVAL` env var. |

All changes keep the public API (`PriceCache.update/get/get_price/get_all/remove/version`,
`MarketDataSource.start/stop/add_ticker/remove_ticker/get_tickers`,
`create_market_data_source`, `create_stream_router`) backward compatible.

---

## 2. File Layout

```
backend/
  app/
    market/
      __init__.py        # Public re-exports
      models.py          # PriceUpdate (frozen dataclass)
      cache.py           # PriceCache (thread-safe store + version counter)
      interface.py       # MarketDataSource ABC
      tickers.py         # [R2] normalize_ticker(), InvalidTickerError
      seed_prices.py     # SEED_PRICES, TICKER_PARAMS, correlation constants
      simulator.py       # GBMSimulator + SimulatorDataSource
      massive_client.py  # MassiveDataSource (+ snapshot parsing helpers)
      factory.py         # create_market_data_source()
      stream.py          # create_stream_router() → GET /api/stream/prices
    services/
      ticker_tracking.py # [R2] watchlist ∪ positions coordination (backend-owned)
  tests/
    market/
      test_models.py  test_cache.py  test_tickers.py  test_simulator.py
      test_simulator_source.py  test_massive.py  test_factory.py  test_stream.py
```

Dependencies (already in `pyproject.toml`): `fastapi`, `uvicorn[standard]`,
`numpy`, `massive`. The dev extras are `pytest`, `pytest-asyncio`
(`asyncio_mode = "auto"`), `pytest-cov`, and `ruff`.

---

## 3. Data Model — `models.py`

`PriceUpdate` is an immutable value object: one ticker's price at one instant,
plus enough context to compute tick-to-tick and daily change.

```python
"""Data models for market data."""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float                         # price at the previous update
    timestamp: float = field(default_factory=time.time)  # Unix seconds
    reference_price: float | None = None          # [R2] prev close / session open

    # --- Tick-to-tick (drives flash animation) ---

    @property
    def change(self) -> float:
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat' relative to the previous update."""
        if self.price > self.previous_price:
            return "up"
        if self.price < self.previous_price:
            return "down"
        return "flat"

    # --- [R2] Daily (drives the watchlist "daily change %" column) ---

    @property
    def day_change(self) -> float:
        if self.reference_price is None:
            return 0.0
        return round(self.price - self.reference_price, 4)

    @property
    def day_change_percent(self) -> float:
        if not self.reference_price:
            return 0.0
        return round((self.price - self.reference_price) / self.reference_price * 100, 4)

    def to_dict(self) -> dict:
        """Single serialization point for SSE and REST responses."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
            "reference_price": self.reference_price,
            "day_change": self.day_change,
            "day_change_percent": self.day_change_percent,
        }
```

Decisions:

- **`frozen=True, slots=True`.** Safe to share across tasks and threads without
  copying, and cheap to create (about 20 per second).
- **Derived values are properties.** `direction` can never disagree with the
  prices it is computed from.
- **`reference_price` semantics** depend on the source and are documented once:
  - Massive: the previous session's close (`prev_day.close`). This is true
    "daily change", matching what a brokerage shows.
  - Simulator: the price when the simulator started tracking the ticker
    ("since session start"). This is the honest equivalent for synthetic data.
- `reference_price` is the last field and has a default, so existing
  constructor calls `PriceUpdate(ticker, price, previous_price, timestamp)` still work.

Example:

```python
>>> u = PriceUpdate("AAPL", price=191.50, previous_price=191.20,
...                 timestamp=1764150000.0, reference_price=190.00)
>>> u.direction, u.change, u.day_change_percent
('up', 0.3, 0.7895)
```

---

## 4. Price Cache — `cache.py`

The single source of truth for "what is the price right now". It is written by
exactly one data source and read by many consumers.

```python
"""Thread-safe in-memory price cache."""

from __future__ import annotations

import time
from collections.abc import Mapping
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Latest PriceUpdate per ticker, safe to use from threads and the event loop.

    Writers: the active MarketDataSource (simulator task or Massive poller).
    Readers: SSE stream, trade execution, portfolio valuation, LLM context.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version = 0  # bumped on every write batch

    # --- Writes ---

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        reference_price: float | None = None,
    ) -> PriceUpdate:
        """Record a new price. Returns the stored PriceUpdate.

        - First update for a ticker: previous_price == price (direction 'flat').
        - reference_price=None keeps the existing reference (or uses `price` on
          the first update), so the simulator only sets it once.
        """
        with self._lock:
            update = self._build(ticker, price, timestamp, reference_price)
            self._prices[ticker] = update
            self._version += 1
            return update

    def update_many(
        self,
        prices: Mapping[str, float],
        timestamp: float | None = None,
    ) -> None:
        """[R2] Write one tick's worth of prices atomically, bumping version once."""
        if not prices:
            return
        with self._lock:
            for ticker, price in prices.items():
                self._prices[ticker] = self._build(ticker, price, timestamp, None)
            self._version += 1

    def remove(self, ticker: str) -> None:
        with self._lock:
            if self._prices.pop(ticker, None) is not None:
                self._version += 1  # [R2] removal is a visible change for SSE

    # --- Reads ---

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_price(self, ticker: str) -> float | None:
        update = self.get(ticker)
        return update.price if update else None

    def get_all(self) -> dict[str, PriceUpdate]:
        """Shallow copy; values are immutable so this is a consistent snapshot."""
        with self._lock:
            return dict(self._prices)

    @property
    def version(self) -> int:
        with self._lock:  # [R2]
            return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices

    # --- Internal (caller holds the lock) ---

    def _build(
        self,
        ticker: str,
        price: float,
        timestamp: float | None,
        reference_price: float | None,
    ) -> PriceUpdate:
        prev = self._prices.get(ticker)
        previous_price = prev.price if prev else price
        if reference_price is None:
            reference_price = (
                prev.reference_price if prev and prev.reference_price is not None else price
            )
        return PriceUpdate(
            ticker=ticker,
            price=round(price, 2),
            previous_price=round(previous_price, 2),
            timestamp=time.time() if timestamp is None else timestamp,  # [R2] 0.0 is valid
            reference_price=round(reference_price, 2),
        )
```

Why `threading.Lock` and not `asyncio.Lock`: the Massive client is synchronous
and runs in `asyncio.to_thread()`, a real OS thread. A `threading.Lock` is held
for microseconds (dict operations only), so blocking the event loop on it is
not a concern.

Why the version counter: the SSE loop wakes every 0.5s. Under Massive, prices
change only every 15s. Comparing `version` lets the stream skip redundant sends
without diffing payloads (§11).

Usage:

```python
cache = PriceCache()
cache.update("AAPL", 190.0)                       # first: flat, ref=190.0
cache.update_many({"AAPL": 190.4, "MSFT": 420.1}) # one version bump
cache.get("AAPL").direction                       # 'up'
cache.get("AAPL").day_change_percent              # 0.2105
cache.get_price("ZZZZ")                           # None → caller decides
```

---

## 5. Unified Interface — `interface.py`

The interface is unchanged from the current code. Only the docstrings are
tightened to state the normalization and idempotency contract.

```python
"""Abstract interface for market data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push PriceUpdates into a shared PriceCache on their own
    schedule. Consumers never ask the source for prices; they read the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL"])   # once
        await source.add_ticker("TSLA")          # any time, idempotent
        await source.remove_ticker("GOOGL")      # any time, idempotent
        await source.stop()                      # idempotent

    All ticker arguments are normalized with normalize_ticker() by the
    implementation; callers may pass "tsla" or " TSLA ".
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing updates. Must not raise on transient data errors."""

    @abstractmethod
    async def stop(self) -> None:
        """Cancel background work. Safe to call more than once."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Start tracking a ticker. No-op if already tracked."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Stop tracking a ticker and drop it from the PriceCache. No-op if absent."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Currently tracked tickers (a copy)."""
```

Why the source writes instead of returning prices: it decouples timing. The
producer's cadence (0.5s or 15s) is independent of the consumer's cadence (SSE
at 0.5s, a trade at any moment). It also keeps the door open for multiple users
reading one cache.

---

## 6. Ticker Normalization — `tickers.py`

**[R2] New module.** It is the single place that decides what a valid ticker
looks like. It is used by both data sources and by the watchlist and trade API
validation, so a bad symbol is rejected with a 400 before it reaches the market
layer.

```python
"""Ticker symbol normalization and validation."""

from __future__ import annotations

import re

# US equities: 1–10 chars, letter first; allow class shares (BRK.B) and dashes (BF-B).
_TICKER_RE = re.compile(r"[A-Z][A-Z0-9.\-]{0,9}")


class InvalidTickerError(ValueError):
    """Raised when a string cannot be a ticker symbol."""


def normalize_ticker(raw: str) -> str:
    """'  aapl ' → 'AAPL'. Raises InvalidTickerError for malformed input."""
    if not isinstance(raw, str):
        raise InvalidTickerError(f"Ticker must be a string, got {type(raw).__name__}")
    ticker = raw.strip().upper()
    if not _TICKER_RE.fullmatch(ticker):
        raise InvalidTickerError(f"Invalid ticker symbol: {raw!r}")
    return ticker
```

```python
>>> normalize_ticker(" brk.b ")
'BRK.B'
>>> normalize_ticker("$AAPL")
InvalidTickerError: Invalid ticker symbol: '$AAPL'
```

This validates *format* only. Whether Massive knows the symbol is discovered on
the next poll (§18.4).

---

## 7. Seed Prices & Parameters — `seed_prices.py`

Constants only, unchanged from the current code, plus one helper. **[R2]** The
helper gives deterministic seed prices for tickers outside the default set.

```python
"""Seed prices and per-ticker parameters for the market simulator."""

import hashlib

SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00,
    "TSLA": 250.00, "NVDA": 800.00, "META": 500.00, "JPM": 195.00,
    "V": 280.00, "NFLX": 600.00,
}

# sigma: annualized volatility; mu: annualized drift
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.50, "mu": 0.03},  # high volatility
    "NVDA":  {"sigma": 0.40, "mu": 0.08},  # high volatility, strong drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},  # low volatility (bank)
    "V":     {"sigma": 0.17, "mu": 0.04},  # low volatility (payments)
    "NFLX":  {"sigma": 0.35, "mu": 0.05},
}
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}
INTRA_TECH_CORR = 0.6
INTRA_FINANCE_CORR = 0.5
CROSS_GROUP_CORR = 0.3   # cross-sector and unknown tickers
TSLA_CORR = 0.3          # TSLA does its own thing


def seed_price_for(ticker: str) -> float:
    """[R2] Known seed, else a stable pseudo-random price in [50, 300).

    Stable across restarts and remove/re-add, so a user-added ticker such as
    PYPL doesn't jump from $83 to $241 between sessions.
    """
    if ticker in SEED_PRICES:
        return SEED_PRICES[ticker]
    digest = hashlib.sha256(ticker.encode()).digest()
    frac = int.from_bytes(digest[:4], "big") / 2**32
    return round(50.0 + frac * 250.0, 2)
```

---

## 8. GBM Simulator — `simulator.py`

Two classes:

- `GBMSimulator` is pure, synchronous math. It holds prices and advances them.
- `SimulatorDataSource` is the `MarketDataSource` adapter. It runs
  `step()` in an asyncio task and writes to the cache.

### 8.1 The math

Geometric Brownian motion, discretized exactly (log-normal step):

```
S(t+Δt) = S(t) · exp( (μ − σ²/2)·Δt + σ·√Δt·Z )
```

- `Δt = 0.5 s / (252 · 6.5 · 3600 s) ≈ 8.48e-8` trading years per tick.
- `Z` is a vector of **correlated** standard normals: `Z = L · ε`, where
  `ε ~ N(0, I)` and `L` is the Cholesky factor of the correlation matrix `C`
  (`C = L·Lᵀ`).
- The per-tick standard deviation of the log return is `σ·√Δt`. For AAPL
  (σ = 0.22) that is ≈ 6.4e-5, about **1.2¢ on a $190 stock**. Prices drift
  visibly within a minute and never explode.
- **Shock events:** with probability `p = 0.001` per ticker per tick, multiply
  by `1 ± U(2%, 5%)`. With 10 tickers at 2 ticks/s, that is about one event every
  50s, which gives the UI something to flash about.

Correlation structure (from `_pairwise_correlation`):

| Pair | ρ |
|---|---|
| tech ↔ tech (AAPL, GOOGL, MSFT, AMZN, META, NVDA, NFLX) | 0.6 |
| finance ↔ finance (JPM, V) | 0.5 |
| anything ↔ TSLA | 0.3 |
| cross-sector / unknown tickers | 0.3 |

**This matrix is always positive definite, so Cholesky never fails.** It
decomposes as `0.3·J + 0.3·J_tech + 0.2·J_fin + D`, where the `J`s are all-ones
blocks (PSD) and `D` is a strictly positive diagonal (0.4 for tech, 0.5 for
finance, 0.7 otherwise). Dynamically added tickers only ever land in the "other"
class, so the property holds for any watchlist. A test pins this (§17).

### 8.2 `GBMSimulator`

```python
"""GBM-based market simulator."""

from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    TICKER_PARAMS,
    TSLA_CORR,
    seed_price_for,
)
from .tickers import normalize_ticker

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Correlated geometric Brownian motion over a dynamic set of tickers."""

    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR   # ≈ 8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
        rng: np.random.Generator | None = None,   # [R2] injectable for tests
    ) -> None:
        self._dt = dt
        self._sqrt_dt = math.sqrt(dt)
        self._event_prob = event_probability
        self._rng = rng or np.random.default_rng()

        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    # --- Public API ---

    def step(self) -> dict[str, float]:
        """Advance every ticker one Δt. Hot path: ~10 tickers, 2×/second."""
        n = len(self._tickers)
        if n == 0:
            return {}

        eps = self._rng.standard_normal(n)
        z = self._cholesky @ eps if self._cholesky is not None else eps
        shocks = self._rng.random(n) < self._event_prob

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            mu = self._params[ticker]["mu"]
            sigma = self._params[ticker]["sigma"]
            drift = (mu - 0.5 * sigma * sigma) * self._dt
            diffusion = sigma * self._sqrt_dt * z[i]
            price = self._prices[ticker] * math.exp(drift + diffusion)

            if shocks[i]:
                magnitude = self._rng.uniform(0.02, 0.05)
                sign = 1 if self._rng.random() < 0.5 else -1
                price *= 1 + sign * magnitude
                logger.debug("Shock on %s: %+.1f%%", ticker, sign * magnitude * 100)

            self._prices[ticker] = price
            result[ticker] = round(price, 2)
        return result

    def add_ticker(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internals ---

    def _add_ticker_internal(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = seed_price_for(ticker)                # [R2]
        self._params[ticker] = dict(TICKER_PARAMS.get(ticker, DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """O(n²) build + O(n³) factorization; n is tens at most, so negligible."""
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho
        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        tech, finance = CORRELATION_GROUPS["tech"], CORRELATION_GROUPS["finance"]
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

Notes on the R2 changes to `step()`:

- It uses a `numpy.random.Generator` (`default_rng`) instead of the global
  `np.random` and `random` modules. Tests pass `np.random.default_rng(42)` for
  fully reproducible paths, including shocks.
- Shock draws are vectorized (`rng.random(n) < p`), one call per tick instead of n.

### 8.3 `SimulatorDataSource`

```python
class SimulatorDataSource(MarketDataSource):
    """MarketDataSource that steps GBMSimulator every `update_interval` seconds."""

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
        rng: np.random.Generator | None = None,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._rng = rng
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        normalized = list(dict.fromkeys(normalize_ticker(t) for t in tickers))  # dedupe, keep order
        self._sim = GBMSimulator(normalized, event_probability=self._event_prob, rng=self._rng)
        # Seed the cache so the first SSE event has every ticker; the seed is
        # also the reference ("session open") price.
        for ticker in normalized:
            price = self._sim.get_price(ticker)
            self._cache.update(ticker, price, reference_price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(normalized))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if self._sim is None or ticker in self._sim.get_tickers():
            return
        self._sim.add_ticker(ticker)
        price = self._sim.get_price(ticker)
        self._cache.update(ticker, price, reference_price=price)  # priced immediately
        logger.info("Simulator: added %s @ %.2f", ticker, price)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    self._cache.update_many(self._sim.step())   # [R2] one version bump
            except Exception:
                logger.exception("Simulator step failed")       # never kill the loop
            await asyncio.sleep(self._interval)
```

Behavior summary:

- The first SSE event after startup already contains every ticker (cache seeded
  in `start()`).
- A ticker added via the watchlist or chat has a price **immediately**, so a
  trade on it right after adding succeeds.
- `step()` runs on the event loop. At 10 tickers it takes about 20µs, so there
  is no need to offload it to a thread.

---

## 9. Massive API Client — `massive_client.py`

### 9.1 API facts this design relies on

Verified against the installed `massive` package (`massive/rest/models/snapshot.py`,
`trades.py`, `aggs.py`):

- `RESTClient(api_key=...).get_snapshot_all(market_type=SnapshotMarketType.STOCKS, tickers=[...])`
  is **one HTTP call** for all tickers: `GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,MSFT`.
  It returns `list[TickerSnapshot]`.
- `TickerSnapshot` fields: `ticker`, `day: Agg | None`, `prev_day: Agg | None`,
  `last_trade: LastTrade | None`, `last_quote`, `min`, `todays_change`,
  `todays_change_percent`, `updated` (ns).
- `LastTrade` has `price`, `size`, `sip_timestamp`, `participant_timestamp`
  (**nanoseconds**). **It has no `timestamp` attribute.**
- `Agg` has `open`, `high`, `low`, `close`, `volume`, `vwap`, `timestamp`.
  **There is no `previous_close`.** The previous close is `prev_day.close`.
- Snapshot data is cleared at 12am ET and repopulated from about 4am ET. Before
  then, `day` values can be `0`/`None` and `last_trade` may be missing for some
  plans.
- The client is synchronous (urllib3) and raises `massive.exceptions.AuthError`
  or `BadResponse` on errors (401/403/429/5xx). It retries 5xx internally.
- Free tier: 5 requests/minute, so poll every ≥ 12s. The default is 15s.

> The archived `MASSIVE_API.md` example uses `snap.day.previous_close` and
> `snap.last_trade.timestamp`. **Both are wrong** for this client and must not be
> copied.

### 9.2 Snapshot parsing (pure functions, unit-testable)

```python
"""Massive (Polygon.io) API client for real market data."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource
from .tickers import normalize_ticker

logger = logging.getLogger(__name__)

MAX_BACKOFF_SECONDS = 120.0


@dataclass(frozen=True, slots=True)
class ParsedQuote:
    ticker: str
    price: float
    timestamp: float               # Unix seconds
    reference_price: float | None  # previous close


def to_unix_seconds(ts: float | int | None) -> float | None:
    """Normalize a Massive timestamp of unknown unit (s/ms/µs/ns) to seconds."""
    if not ts:
        return None
    ts = float(ts)
    if ts > 1e17:   # nanoseconds  (~1.7e18 today)
        return ts / 1e9
    if ts > 1e14:   # microseconds
        return ts / 1e6
    if ts > 1e11:   # milliseconds
        return ts / 1e3
    return ts       # seconds


def _positive(value: float | None) -> float | None:
    return value if isinstance(value, (int, float)) and value > 0 else None


def parse_snapshot(snap) -> ParsedQuote | None:
    """Extract (price, timestamp, previous close) from a TickerSnapshot.

    Price precedence: last trade → today's close-so-far → previous close.
    Returns None if no usable price exists (logged by the caller).
    """
    if not getattr(snap, "ticker", None):
        return None
    last_trade = snap.last_trade
    day = snap.day
    prev_day = snap.prev_day

    price = (
        _positive(last_trade.price if last_trade else None)
        or _positive(day.close if day else None)
        or _positive(prev_day.close if prev_day else None)
    )
    if price is None:
        return None

    ts = (
        to_unix_seconds(last_trade.sip_timestamp if last_trade else None)
        or to_unix_seconds(last_trade.participant_timestamp if last_trade else None)
        or to_unix_seconds(snap.updated)
        or time.time()
    )
    reference = _positive(prev_day.close if prev_day else None)
    return ParsedQuote(snap.ticker.upper(), float(price), ts, reference)
```

Example input/output (raw JSON as the client receives it):

```python
from massive.rest.models import TickerSnapshot

snap = TickerSnapshot.from_dict({
    "ticker": "AAPL",
    "lastTrade": {"p": 191.23, "s": 100, "t": 1764150000123456789},
    "day": {"o": 189.9, "h": 191.8, "l": 189.5, "c": 191.2, "v": 41_000_000},
    "prevDay": {"c": 189.0},
    "updated": 1764150000200000000,
})
parse_snapshot(snap)
# ParsedQuote(ticker='AAPL', price=191.23, timestamp=1764150000.1234567,
#             reference_price=189.0)
```

### 9.3 `MassiveDataSource`

```python
class MassiveDataSource(MarketDataSource):
    """Polls the Massive snapshot endpoint for all tracked tickers in one call."""

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None
        self._consecutive_failures = 0

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(dict.fromkeys(normalize_ticker(t) for t in tickers))
        # Immediate first poll so the cache is warm before the first SSE client.
        # _poll_once never raises; a bad key just leaves the cache empty + logs.
        await self._poll_once()
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info("Massive poller started: %d tickers, %.1fs interval",
                    len(self._tickers), self._interval)

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if ticker not in self._tickers:
            # Rebind instead of mutating: a poll running in a worker thread holds
            # its own tuple copy, so there is no shared mutable state (§9.4).
            self._tickers = [*self._tickers, ticker]
            logger.info("Massive: added %s (priced on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internal ---

    def _next_delay(self) -> float:
        """Normal interval, or exponential backoff after failures (capped)."""
        if self._consecutive_failures == 0:
            return self._interval
        return min(self._interval * 2 ** self._consecutive_failures, MAX_BACKOFF_SECONDS)

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._next_delay())
            await self._poll_once()

    async def _poll_once(self) -> bool:
        """One poll cycle. Returns True on success. Never raises (except cancel)."""
        tickers = tuple(self._tickers)   # immutable snapshot for the worker thread
        if not tickers or self._client is None:
            return True
        try:
            snapshots = await asyncio.to_thread(self._fetch_snapshots, tickers)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # AuthError, BadResponse (429/5xx), network errors
            self._consecutive_failures += 1
            logger.error("Massive poll failed (%d in a row, next try in %.0fs): %s",
                         self._consecutive_failures, self._next_delay(), exc)
            return False

        self._consecutive_failures = 0
        tracked = set(self._tickers)     # re-read: a ticker may have been removed mid-poll
        updated: set[str] = set()
        for snap in snapshots:
            quote = parse_snapshot(snap)
            if quote is None:
                logger.warning("Massive: no usable price for %s",
                               getattr(snap, "ticker", "???"))
                continue
            if quote.ticker not in tracked:
                continue                 # removed while the request was in flight
            self._cache.update(quote.ticker, quote.price,
                               timestamp=quote.timestamp,
                               reference_price=quote.reference_price)
            updated.add(quote.ticker)

        missing = tracked - updated
        if missing:
            logger.debug("Massive: no data for %s (unknown symbol or pre-market)",
                         sorted(missing))
        return True

    def _fetch_snapshots(self, tickers: tuple[str, ...]) -> list:
        """Blocking HTTP call; runs in a worker thread via asyncio.to_thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=list(tickers),
        )
```

### 9.4 Concurrency reasoning

- The event loop owns `self._tickers`. The list is only **rebound**, never
  mutated in place, and the worker thread receives a `tuple` copy. The thread
  therefore never sees a list being changed underneath it.
- The thread returns data and the loop writes to the cache. Cache writes happen
  on the event loop anyway, and the lock makes them safe either way.
- Results for a ticker removed while the request was in flight are dropped, so
  a deleted ticker never "comes back" in the cache.

### 9.5 Rate budget

| Plan | `MASSIVE_POLL_INTERVAL` | Calls/min | Headroom |
|---|---|---|---|
| Free (5/min) | 15 (default) | 4 | 1 spare |
| Starter+ (unlimited) | 2–5 | 12–30 | fine |

Adding a ticker does **not** trigger an extra call. On the free tier that would
risk a 429. The new ticker is priced on the next poll (≤ 15s), and until then the
watchlist shows `price: null` (§16).

---

## 10. Factory & Configuration — `factory.py`

```python
"""Factory for creating market data sources."""

from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)

DEFAULT_MASSIVE_POLL_INTERVAL = 15.0
MIN_MASSIVE_POLL_INTERVAL = 1.0


def _poll_interval_from_env() -> float:
    raw = os.environ.get("MASSIVE_POLL_INTERVAL", "").strip()
    if not raw:
        return DEFAULT_MASSIVE_POLL_INTERVAL
    try:
        return max(float(raw), MIN_MASSIVE_POLL_INTERVAL)
    except ValueError:
        logger.warning("Ignoring invalid MASSIVE_POLL_INTERVAL=%r", raw)
        return DEFAULT_MASSIVE_POLL_INTERVAL


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """MASSIVE_API_KEY set and non-empty → Massive; otherwise → simulator.

    Returns an unstarted source; the caller awaits source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        interval = _poll_interval_from_env()
        logger.info("Market data source: Massive API (poll every %.1fs)", interval)
        return MassiveDataSource(api_key=api_key, price_cache=price_cache,
                                 poll_interval=interval)
    logger.info("Market data source: GBM simulator")
    return SimulatorDataSource(price_cache=price_cache)
```

`.env` examples:

```bash
# Simulator (default): leave empty or omit
MASSIVE_API_KEY=

# Real data, free tier
MASSIVE_API_KEY=your-key
MASSIVE_POLL_INTERVAL=15

# Real data, paid tier
MASSIVE_API_KEY=your-key
MASSIVE_POLL_INTERVAL=3
```

---

## 11. SSE Streaming — `stream.py`

### 11.1 Endpoint

`GET /api/stream/prices` returns a `text/event-stream` response. The server
checks the cache every 0.5s and sends a full snapshot whenever `version` has
changed.

```python
"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",  # disable proxy buffering (nginx, App Runner)
}


def create_stream_router(
    price_cache: PriceCache,
    interval: float = 0.5,
    keepalive: float = 15.0,
) -> APIRouter:
    """Build a fresh router bound to `price_cache` (safe to call repeatedly)."""
    router = APIRouter(prefix="/api/stream", tags=["streaming"])  # [R2] per call

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            generate_price_events(price_cache, request, interval, keepalive),
            media_type="text/event-stream",
            headers=SSE_HEADERS,
        )

    return router


def format_prices_event(price_cache: PriceCache) -> str | None:
    prices = price_cache.get_all()
    if not prices:
        return None
    payload = {ticker: update.to_dict() for ticker, update in prices.items()}
    return f"event: prices\ndata: {json.dumps(payload, separators=(',', ':'))}\n\n"


async def generate_price_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
    keepalive: float = 15.0,
) -> AsyncGenerator[str, None]:
    """Yield SSE frames until the client disconnects."""
    yield "retry: 1000\n\n"            # browser reconnects 1s after a drop
    client = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client)

    last_version = -1                  # forces an immediate first snapshot
    last_sent = time.monotonic()
    try:
        while not await request.is_disconnected():
            version = price_cache.version
            if version != last_version:
                last_version = version
                frame = format_prices_event(price_cache)
                if frame:
                    yield frame
                    last_sent = time.monotonic()
            elif time.monotonic() - last_sent >= keepalive:
                yield ": keepalive\n\n"  # [R2] comment frame; ignored by EventSource
                last_sent = time.monotonic()
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled: %s", client)
        raise                          # [R2] let Starlette finish cleanup
    finally:
        logger.info("SSE client disconnected: %s", client)
```

> `_generate_events` (current name) is renamed `generate_price_events` and made
> public so tests can drive it directly (§17.6). Keep `_generate_events = generate_price_events`
> as an alias for one release if anything imports it.

### 11.2 Wire format

Named event `prices` (**[R2]**: the frontend must use
`addEventListener("prices", ...)`; see §16). The data is a JSON object keyed by
ticker:

```
retry: 1000

event: prices
data: {"AAPL":{"ticker":"AAPL","price":190.42,"previous_price":190.38,"timestamp":1764150000.51,"change":0.04,"change_percent":0.021,"direction":"up","reference_price":190.0,"day_change":0.42,"day_change_percent":0.221},"MSFT":{...}}

: keepalive

```

Each event carries **every** tracked ticker (about 10 × 250 bytes ≈ 2.5 KB per
0.5s). This is deliberately simple:

- A reconnecting client is fully up to date after one event. There is no replay
  or `Last-Event-ID` logic.
- Tickers that vanish (removed from tracking) simply stop appearing.
- The payload includes position-only tickers (held but not watched). The frontend
  uses those for live position P&L and filters to the watchlist for the
  watchlist panel.

### 11.3 Why poll-the-cache instead of pub/sub

A per-client `asyncio.Queue` fed by the source would save at most 0.5s of
latency. It would cost fan-out code, back-pressure handling, and per-client
cleanup. At 2 ticks/second, a 0.5s version check is indistinguishable to a
human and costs nothing when idle.

---

## 12. Package Exports — `__init__.py`

```python
"""Market data subsystem for FinAlly."""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router
from .tickers import InvalidTickerError, normalize_ticker

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
    "normalize_ticker",
    "InvalidTickerError",
]
```

Code outside `app/market/` imports only from `app.market`, never from submodules.

---

## 13. FastAPI Integration

### 13.1 App factory and lifespan (`backend/app/main.py`)

Create the cache and source eagerly so routers can be registered at
construction time. Start and stop them in `lifespan`.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.db import init_db, get_tracked_tickers        # backend DB layer (§14)
from app.market import PriceCache, create_market_data_source, create_stream_router
from app.routes import chat, health, portfolio, watchlist
from app.services.snapshots import snapshot_loop        # 30s portfolio snapshots


def create_app() -> FastAPI:
    price_cache = PriceCache()
    source = create_market_data_source(price_cache)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_db()                                   # lazy schema + seed
        await source.start(get_tracked_tickers())   # watchlist ∪ open positions
        snapshots = asyncio.create_task(snapshot_loop(app), name="portfolio-snapshots")
        try:
            yield
        finally:
            snapshots.cancel()
            await source.stop()

    app = FastAPI(title="FinAlly", lifespan=lifespan)
    app.state.price_cache = price_cache
    app.state.market_source = source

    app.include_router(create_stream_router(price_cache))
    app.include_router(portfolio.router)
    app.include_router(watchlist.router)
    app.include_router(chat.router)
    app.include_router(health.router)
    # Static export last so /api/* wins
    app.mount("/", StaticFiles(directory="static", html=True), name="static")
    return app


app = create_app()
```

### 13.2 Dependencies (`backend/app/dependencies.py`)

```python
from fastapi import Request

from app.market import MarketDataSource, PriceCache


def get_price_cache(request: Request) -> PriceCache:
    return request.app.state.price_cache


def get_market_source(request: Request) -> MarketDataSource:
    return request.app.state.market_source
```

Route handlers take `cache: PriceCache = Depends(get_price_cache)`. Tests
override the dependency or build an app with `create_app()` and a monkeypatched
environment.

---

## 14. Ticker Tracking: Watchlist ∪ Positions

**Rule:** the data source tracks every ticker that is **in the watchlist or held
in a position with quantity > 0**. Without a live price, a held position cannot
be valued, sold, or included in portfolio snapshots.

One backend-owned module (`backend/app/services/ticker_tracking.py`) is the only
code that calls `add_ticker`/`remove_ticker`. Watchlist routes, trade routes,
and the LLM chat executor all go through it, so the rule lives in one place.

```python
"""Keeps the market data source's ticker set = watchlist ∪ open positions."""

from __future__ import annotations

from app import db
from app.market import MarketDataSource, normalize_ticker


async def ensure_tracked(source: MarketDataSource, ticker: str) -> str:
    """Start tracking `ticker` if needed. Returns the normalized symbol."""
    ticker = normalize_ticker(ticker)
    if ticker not in source.get_tickers():
        await source.add_ticker(ticker)
    return ticker


async def release_if_unused(source: MarketDataSource, ticker: str) -> None:
    """Stop tracking `ticker` only if it is neither watched nor held."""
    ticker = normalize_ticker(ticker)
    if db.is_in_watchlist(ticker):
        return
    position = db.get_position(ticker)
    if position is not None and position.quantity > 0:
        return
    await source.remove_ticker(ticker)
```

```python
# app/db helper used at startup
def get_tracked_tickers(user_id: str = "default") -> list[str]:
    rows = conn.execute(
        """
        SELECT ticker FROM watchlist WHERE user_id = ?
        UNION
        SELECT ticker FROM positions WHERE user_id = ? AND quantity > 0
        """,
        (user_id, user_id),
    ).fetchall()
    return [r[0] for r in rows]
```

Event flows:

```
POST /api/watchlist {ticker:"pypl"}
  → t = normalize_ticker("pypl")            # 400 on InvalidTickerError
  → INSERT watchlist (409 if duplicate)
  → await ensure_tracked(source, t)
      simulator: priced immediately
      massive:   priced on next poll (≤ MASSIVE_POLL_INTERVAL)
  → 201 {ticker:"PYPL", price: cache.get_price("PYPL")}   # may be null on Massive

DELETE /api/watchlist/NVDA
  → DELETE watchlist row
  → await release_if_unused(source, "NVDA")  # still tracked if shares held

POST /api/portfolio/trade {ticker:"AMD", side:"buy", quantity:5}
  → t = await ensure_tracked(source, "AMD")  # buy of an unwatched ticker is allowed
  → price = cache.get_price(t)  →  None ⇒ 409 "No live price for AMD yet"
  → execute; snapshot portfolio

POST /api/portfolio/trade {side:"sell"} that brings quantity to 0
  → execute; delete position row
  → await release_if_unused(source, t)
```

---

## 15. Downstream Consumers

### 15.1 Trade execution (price lookup)

```python
from fastapi import APIRouter, Depends, HTTPException
from app.dependencies import get_market_source, get_price_cache
from app.market import InvalidTickerError, MarketDataSource, PriceCache
from app.services.ticker_tracking import ensure_tracked

router = APIRouter(prefix="/api/portfolio")


@router.post("/trade")
async def trade(
    req: TradeRequest,
    cache: PriceCache = Depends(get_price_cache),
    source: MarketDataSource = Depends(get_market_source),
):
    try:
        ticker = await ensure_tracked(source, req.ticker)
    except InvalidTickerError as e:
        raise HTTPException(400, str(e))

    quote = cache.get(ticker)
    if quote is None:
        raise HTTPException(409, f"No live price for {ticker} yet; try again shortly")

    # Fill at quote.price. Validation (cash / shares) is the portfolio service's job.
    result = execute_market_order(ticker, req.side, req.quantity, fill_price=quote.price)
    return result
```

The fill price is read once and used for the whole order, so a tick arriving
mid-request cannot split the fill.

### 15.2 Portfolio valuation

```python
def value_portfolio(cash: float, positions: list[Position], cache: PriceCache) -> dict:
    prices = cache.get_all()                     # one consistent snapshot
    rows, total = [], cash
    for p in positions:
        quote = prices.get(p.ticker)
        current = quote.price if quote else p.avg_cost   # fallback: never crash valuation
        market_value = p.quantity * current
        total += market_value
        rows.append({
            "ticker": p.ticker,
            "quantity": p.quantity,
            "avg_cost": p.avg_cost,
            "current_price": current,
            "market_value": round(market_value, 2),
            "unrealized_pnl": round((current - p.avg_cost) * p.quantity, 2),
            "pnl_percent": round((current / p.avg_cost - 1) * 100, 2) if p.avg_cost else 0.0,
            "price_stale": quote is None,
        })
    return {"cash": round(cash, 2), "total_value": round(total, 2), "positions": rows}
```

The same function is used by `GET /api/portfolio`, the 30s snapshot task, the
post-trade snapshot, and the LLM context builder.

### 15.3 Watchlist GET

```python
@router.get("/api/watchlist")
async def get_watchlist(cache: PriceCache = Depends(get_price_cache)):
    prices = cache.get_all()
    return [
        {"ticker": t, **(prices[t].to_dict() if t in prices else {"price": None})}
        for t in db.list_watchlist()
    ]
```

### 15.4 LLM context

```python
def market_context(cache: PriceCache, watchlist: list[str]) -> str:
    prices = cache.get_all()
    lines = []
    for t in watchlist:
        u = prices.get(t)
        lines.append(f"{t}: ${u.price:.2f} ({u.day_change_percent:+.2f}% today)"
                     if u else f"{t}: price unavailable")
    return "\n".join(lines)
```

---

## 16. Frontend Contract

TypeScript types mirroring `PriceUpdate.to_dict()`:

```ts
export type Direction = "up" | "down" | "flat";

export interface PriceUpdate {
  ticker: string;
  price: number;
  previous_price: number;
  timestamp: number;            // Unix seconds
  change: number;               // vs previous tick
  change_percent: number;
  direction: Direction;
  reference_price: number | null;
  day_change: number;           // vs previous close (Massive) / session open (sim)
  day_change_percent: number;
}

export type PricesEvent = Record<string, PriceUpdate>;
```

Connection handling with the status dot:

```ts
type Status = "connected" | "reconnecting" | "disconnected";

export function connectPrices(
  onPrices: (p: PricesEvent) => void,
  onStatus: (s: Status) => void,
): () => void {
  const es = new EventSource("/api/stream/prices");
  es.onopen = () => onStatus("connected");
  es.addEventListener("prices", (e) => onPrices(JSON.parse((e as MessageEvent).data)));
  es.onerror = () =>
    onStatus(es.readyState === EventSource.CLOSED ? "disconnected" : "reconnecting");
  return () => es.close();
}
```

Frontend rules:

- **Flash** when `direction !== "flat"` *and* `timestamp` changed since the last
  event for that ticker. Under Massive, the same snapshot can be re-sent after
  another ticker updates, and it must not re-flash.
- **Sparklines** append `price` only when `timestamp` changes for that ticker.
- **Daily change %** column = `day_change_percent`.
- A watchlist ticker absent from the event (Massive, just added) renders `—`.

---

## 17. Testing

All tests run under `pytest` with `asyncio_mode = "auto"`. Commands:

```bash
cd backend
uv run --extra dev pytest -v
uv run --extra dev pytest --cov=app --cov-report=term-missing
uv run --extra dev ruff check app/ tests/
```

### 17.1 `test_models.py` (additions)

```python
from app.market import PriceUpdate


def test_day_change_uses_reference_price():
    u = PriceUpdate("AAPL", price=191.5, previous_price=191.2,
                    timestamp=0.0, reference_price=190.0)
    assert u.day_change == 1.5
    assert u.day_change_percent == round(1.5 / 190 * 100, 4)


def test_day_change_zero_without_reference():
    u = PriceUpdate("AAPL", 191.5, 191.2)
    assert u.day_change == 0.0 and u.day_change_percent == 0.0


def test_to_dict_has_daily_fields():
    d = PriceUpdate("AAPL", 1.0, 1.0, 0.0, 1.0).to_dict()
    assert {"reference_price", "day_change", "day_change_percent"} <= d.keys()
```

### 17.2 `test_cache.py` (additions)

```python
import threading
from app.market import PriceCache


def test_reference_price_sticky_after_first_update():
    c = PriceCache()
    c.update("AAPL", 190.0)
    c.update("AAPL", 192.0)
    assert c.get("AAPL").reference_price == 190.0


def test_explicit_reference_overrides():
    c = PriceCache()
    c.update("AAPL", 190.0, reference_price=185.0)
    assert c.get("AAPL").day_change == 5.0


def test_update_many_bumps_version_once():
    c = PriceCache()
    v0 = c.version
    c.update_many({"AAPL": 1.0, "MSFT": 2.0, "V": 3.0})
    assert c.version == v0 + 1 and len(c) == 3


def test_zero_timestamp_preserved():
    c = PriceCache()
    assert c.update("AAPL", 1.0, timestamp=0.0).timestamp == 0.0


def test_remove_bumps_version_only_when_present():
    c = PriceCache()
    c.update("AAPL", 1.0)
    v = c.version
    c.remove("ZZZ")
    assert c.version == v
    c.remove("AAPL")
    assert c.version == v + 1


def test_concurrent_writers():
    c = PriceCache()

    def writer(t):
        for i in range(1000):
            c.update(t, 100 + i)

    threads = [threading.Thread(target=writer, args=(f"T{i}",)) for i in range(8)]
    for th in threads: th.start()
    for th in threads: th.join()
    assert len(c) == 8 and c.version == 8000
```

### 17.3 `test_tickers.py` (new)

```python
import pytest
from app.market import InvalidTickerError, normalize_ticker


@pytest.mark.parametrize("raw,expected", [("aapl", "AAPL"), (" brk.b ", "BRK.B"), ("BF-B", "BF-B")])
def test_normalizes(raw, expected):
    assert normalize_ticker(raw) == expected


@pytest.mark.parametrize("raw", ["", "  ", "$AAPL", "1ABC", "TOOLONGTICKER", "AA PL", None])
def test_rejects(raw):
    with pytest.raises(InvalidTickerError):
        normalize_ticker(raw)
```

### 17.4 `test_simulator.py` (additions)

```python
import numpy as np
from app.market.seed_prices import SEED_PRICES, seed_price_for
from app.market.simulator import GBMSimulator

DEFAULTS = list(SEED_PRICES)


def test_cholesky_full_default_set_plus_unknowns():
    sim = GBMSimulator(DEFAULTS + ["PYPL", "AMD", "UBER", "COIN"])
    assert sim._cholesky is not None          # positive definite → factorized


def test_reproducible_with_seeded_rng():
    a = GBMSimulator(DEFAULTS, rng=np.random.default_rng(42))
    b = GBMSimulator(DEFAULTS, rng=np.random.default_rng(42))
    for _ in range(100):
        assert a.step() == b.step()


def test_seed_price_deterministic_for_unknown():
    assert seed_price_for("PYPL") == seed_price_for("PYPL")
    assert 50 <= seed_price_for("PYPL") < 300


def test_tech_more_correlated_than_cross_sector():
    sim = GBMSimulator(["AAPL", "MSFT", "JPM"], rng=np.random.default_rng(0))
    logs = {t: [] for t in sim.get_tickers()}
    prev = {t: sim.get_price(t) for t in logs}
    for _ in range(5000):
        for t, p in sim.step().items():
            logs[t].append(np.log(sim.get_price(t) / prev[t]))
            prev[t] = sim.get_price(t)
    r = np.corrcoef([logs["AAPL"], logs["MSFT"], logs["JPM"]])
    assert r[0, 1] > r[0, 2] + 0.15            # ≈0.6 vs ≈0.3


def test_shocks_fire_at_probability_one():
    sim = GBMSimulator(["AAPL"], event_probability=1.0, rng=np.random.default_rng(1))
    before = sim.get_price("AAPL")
    after = sim.step()["AAPL"]
    assert abs(after / before - 1) >= 0.019
```

### 17.5 `test_massive.py`: parse **real** model objects

Replace the `MagicMock` snapshots that use `last_trade.timestamp` with objects
built through the library's own `from_dict`. That closes the gap that hid bug #1.

```python
from unittest.mock import MagicMock

import pytest
from massive.rest.models import TickerSnapshot

from app.market import PriceCache
from app.market.massive_client import MassiveDataSource, parse_snapshot, to_unix_seconds


def snap(ticker="AAPL", p=191.23, t=1764150000123456789, day_c=191.2, prev_c=189.0, **extra):
    d = {"ticker": ticker, "day": {"c": day_c}, "prevDay": {"c": prev_c}, **extra}
    if p is not None:
        d["lastTrade"] = {"p": p, "t": t}
    return TickerSnapshot.from_dict(d)


def test_parse_real_snapshot():
    q = parse_snapshot(snap())
    assert q.ticker == "AAPL" and q.price == 191.23 and q.reference_price == 189.0
    assert q.timestamp == pytest.approx(1764150000.123, abs=1e-3)


def test_parse_falls_back_to_day_close_then_prev_close():
    assert parse_snapshot(snap(p=None)).price == 191.2
    assert parse_snapshot(snap(p=None, day_c=0)).price == 189.0


def test_parse_returns_none_without_any_price():
    assert parse_snapshot(snap(p=None, day_c=0, prev_c=0)) is None


@pytest.mark.parametrize("raw,secs", [
    (1764150000, 1764150000), (1764150000_123, 1764150000.123),
    (1764150000_123456, 1764150000.123456), (1764150000_123456789, 1764150000.123456789),
])
def test_timestamp_units(raw, secs):
    assert to_unix_seconds(raw) == pytest.approx(secs)


async def test_poll_once_updates_cache_with_reference():
    cache = PriceCache()
    src = MassiveDataSource("key", cache)
    src._client = MagicMock()
    src._client.get_snapshot_all.return_value = [snap(), snap("MSFT", p=421.0, prev_c=420.0)]
    src._tickers = ["AAPL", "MSFT"]
    assert await src._poll_once() is True
    assert cache.get("AAPL").price == 191.23
    assert cache.get("MSFT").day_change == 1.0


async def test_failure_triggers_backoff():
    src = MassiveDataSource("key", PriceCache(), poll_interval=15)
    src._client = MagicMock()
    src._client.get_snapshot_all.side_effect = RuntimeError("429")
    src._tickers = ["AAPL"]
    assert await src._poll_once() is False
    assert src._next_delay() == 30
    await src._poll_once(); await src._poll_once(); await src._poll_once()
    assert src._next_delay() == 120          # capped


async def test_result_for_removed_ticker_is_dropped():
    cache = PriceCache()
    src = MassiveDataSource("key", cache)
    src._client = MagicMock()
    src._tickers = ["AAPL"]

    def fetch(**_):
        src._tickers = []                     # removed while request in flight
        return [snap()]

    src._client.get_snapshot_all.side_effect = fetch
    await src._poll_once()
    assert cache.get("AAPL") is None


async def test_add_ticker_normalizes():
    src = MassiveDataSource("key", PriceCache())
    await src.add_ticker(" pypl ")
    assert src.get_tickers() == ["PYPL"]
```

### 17.6 `test_stream.py` (new)

The generator is driven directly with a fake `Request`, with no server needed.
One more test covers the HTTP wiring.

```python
import json

from fastapi import FastAPI
from app.market import PriceCache, create_stream_router
from app.market.stream import generate_price_events


class FakeRequest:
    client = None

    def __init__(self, polls_before_disconnect: int):
        self._left = polls_before_disconnect

    async def is_disconnected(self) -> bool:
        self._left -= 1
        return self._left < 0


async def collect(cache, polls, **kw):
    return [f async for f in generate_price_events(cache, FakeRequest(polls), interval=0, **kw)]


async def test_first_frames_retry_then_snapshot():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    frames = await collect(cache, polls=1)
    assert frames[0] == "retry: 1000\n\n"
    assert frames[1].startswith("event: prices\ndata: ")
    payload = json.loads(frames[1].split("data: ", 1)[1])
    assert payload["AAPL"]["price"] == 190.0


async def test_no_resend_when_version_unchanged():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    frames = await collect(cache, polls=5, keepalive=999)
    assert sum(f.startswith("event: prices") for f in frames) == 1


async def test_keepalive_when_idle():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    frames = await collect(cache, polls=3, keepalive=0)
    assert ": keepalive\n\n" in frames


def test_router_factory_is_idempotent():
    cache = PriceCache()
    app = FastAPI()
    app.include_router(create_stream_router(cache))
    app.include_router(create_stream_router(cache))   # no duplicate-route footgun
    paths = [r.path for r in create_stream_router(cache).routes]
    assert paths == ["/api/stream/prices"]
```

### 17.7 `test_simulator_source.py` (additions)

```python
import asyncio
from app.market import PriceCache
from app.market.simulator import SimulatorDataSource


async def test_add_ticker_is_priced_immediately_and_normalized():
    cache = PriceCache()
    src = SimulatorDataSource(cache, update_interval=0.01)
    await src.start(["AAPL"])
    await src.add_ticker("pypl")
    assert "PYPL" in src.get_tickers() and cache.get_price("PYPL") is not None
    assert "pypl" not in src.get_tickers()
    await src.stop()


async def test_reference_price_is_session_open():
    cache = PriceCache()
    src = SimulatorDataSource(cache, update_interval=0.01)
    await src.start(["AAPL"])
    await asyncio.sleep(0.1)
    assert cache.get("AAPL").reference_price == 190.0
    await src.stop()
```

### 17.8 Live smoke test (manual, optional)

Run this with a real key to check the parser against the live API before
shipping real-data mode:

```bash
cd backend
MASSIVE_API_KEY=... uv run python -c "
import asyncio; from app.market import PriceCache; from app.market.massive_client import MassiveDataSource
async def main():
    c = PriceCache(); s = MassiveDataSource(__import__('os').environ['MASSIVE_API_KEY'], c)
    await s.start(['AAPL','MSFT']); print({t: u.to_dict() for t, u in c.get_all().items()}); await s.stop()
asyncio.run(main())"
```

---

## 18. Error Handling & Edge Cases

| # | Situation | Behavior |
|---|-----------|----------|
| 18.1 | Empty watchlist and no positions at startup | Source starts with `[]`. The simulator `step()` returns `{}`, Massive skips the call. SSE sends only `retry` and keepalives. Adding a ticker later works normally. |
| 18.2 | Trade on a ticker with no cached price | `ensure_tracked` first. The simulator prices it instantly. If the price is still `None` (Massive before next poll), return **409** with a "try again shortly" message. The LLM chat surfaces the same message in its response. |
| 18.3 | Invalid Massive key (401) / plan lacks endpoint (403) | `start()` does not raise. The error is logged, backoff grows to 120s, and the cache stays empty. The app runs and the UI shows `—` prices. The log line says exactly why. Fix: correct the key or unset it to use the simulator. |
| 18.4 | Unknown symbol on Massive (e.g. `XYZQ`) | Passes format validation. The snapshot returns nothing for it, a debug log lists it as missing, and the watchlist shows `—`. Not auto-removed: the user (or LLM) can remove it. |
| 18.5 | 429 rate limit | Counts as a failure, so exponential backoff 30 → 60 → 120s, and it resets after the first success. |
| 18.6 | Market closed / pre-market | `last_trade.price` is the last trade (may be after-hours). If `day` is zeroed after midnight ET, the price falls back to `prev_day.close`. `day_change_percent` then reads 0%, which is correct. |
| 18.7 | Ticker removed from watchlist while held | `release_if_unused` keeps it tracked. It disappears from the watchlist panel but keeps streaming for the positions table. |
| 18.8 | Ticker removed while a Massive poll is in flight | Result dropped (§9.4). No resurrected cache entry. |
| 18.9 | Simulator step raises (should never happen) | `logger.exception` and the loop continues on the next tick. |
| 18.10 | SSE client disconnects / tab closed | `is_disconnected()` ends the loop. Server shutdown cancels the generator and `CancelledError` propagates. No leaked tasks. |
| 18.11 | Proxy idle timeout (App Runner ≈ 120s, nginx 60s) | `: keepalive` every 15s holds the connection open during idle Massive periods. |
| 18.12 | Price precision | The cache rounds to cents for display and fills. The simulator keeps full-precision internal state, so rounding never compounds. P&L math uses the rounded fill price stored in `trades`. |
| 18.13 | Duplicate tickers in `start()` | Deduplicated while preserving order (`dict.fromkeys`). |
| 18.14 | Non-positive or `None` prices from the API | Rejected by `_positive()`. That snapshot is skipped with a warning. |

---

## 19. Configuration Reference

| Setting | Where | Default | Notes |
|---|---|---|---|
| `MASSIVE_API_KEY` | env | empty | Non-empty selects Massive. Whitespace-only counts as empty. |
| `MASSIVE_POLL_INTERVAL` | env | `15` | Seconds, min 1. Use ≥ 12 on the free tier. |
| Simulator tick | `SimulatorDataSource(update_interval=)` | `0.5` s | Also implies `dt`. |
| Shock probability | `SimulatorDataSource(event_probability=)` | `0.001` | Per ticker per tick. |
| SSE check interval | `create_stream_router(interval=)` | `0.5` s | |
| SSE keepalive | `create_stream_router(keepalive=)` | `15` s | |
| SSE client retry | hard-coded frame | `1000` ms | |
| Massive max backoff | `MAX_BACKOFF_SECONDS` | `120` s | |

Add `MASSIVE_POLL_INTERVAL` (commented) to `.env.example`.

---

## 20. Implementation Checklist

Ordered so that each step leaves the suite green.

1. **`tickers.py`**: add `normalize_ticker` and `InvalidTickerError`, plus `test_tickers.py`.
2. **`models.py`**: add `reference_price`, `day_change`, `day_change_percent`, and
   extend `to_dict()`. Update `test_models.py`.
3. **`cache.py`**: add the `reference_price` param and `update_many()`, lock the
   `version` read, use `timestamp is None`, and bump the version on remove. Update `test_cache.py`.
4. **`seed_prices.py`**: add `seed_price_for()`.
5. **`simulator.py`**: injectable `rng`, vectorized shocks, `seed_price_for`, and
   normalization in the data source. Set the reference price on start/add and use
   `update_many` in the loop. Update the simulator tests.
6. **`massive_client.py`** (**highest priority — real-data mode is broken today**):
   add `parse_snapshot` and `to_unix_seconds`, the tuple snapshot for the thread,
   drop results for removed tickers, add backoff, and return a bool from
   `_poll_once`. Rewrite the `test_massive.py` fixtures to use `TickerSnapshot.from_dict`.
7. **`factory.py`**: `MASSIVE_POLL_INTERVAL`. Add a factory test for the
   invalid/min values.
8. **`stream.py`**: a router per call, named `prices` event, keepalive, re-raise
   `CancelledError`, and a public `generate_price_events`. Add `test_stream.py`.
9. **`__init__.py`**: export `normalize_ticker` and `InvalidTickerError`.
10. Update `backend/CLAUDE.md` (API list, new fields, env var) and
    `planning/MARKET_DATA_SUMMARY.md` (status and test count).
11. Backend agent: implement §13–§15 (`create_app`, dependencies,
    `ticker_tracking`, consumers) when building the portfolio, watchlist, and chat routes.
12. Frontend agent: consume §16 (named `prices` event, `day_change_percent`,
    flash/sparkline de-duplication by `timestamp`).
