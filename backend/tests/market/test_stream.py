"""Tests for the SSE streaming generator and router."""

import asyncio
import json
from types import SimpleNamespace

from fastapi import FastAPI

from app.market.cache import PriceCache
from app.market.stream import _generate_events, create_stream_router


class FakeRequest:
    """Request stub that disconnects after N is_disconnected() checks."""

    def __init__(self, disconnect_after: int, client=SimpleNamespace(host="1.2.3.4")):
        self._left = disconnect_after
        self.client = client

    async def is_disconnected(self) -> bool:
        if self._left <= 0:
            return True
        self._left -= 1
        return False


async def _collect(cache, request, interval=0.001):
    return [chunk async for chunk in _generate_events(cache, request, interval=interval)]


class TestGenerateEvents:
    async def test_first_chunk_is_retry_directive(self):
        chunks = await _collect(PriceCache(), FakeRequest(0))
        assert chunks == ["retry: 1000\n\n"]

    async def test_emits_all_prices(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        cache.update("GOOGL", 175.0)
        chunks = await _collect(cache, FakeRequest(1))
        assert len(chunks) == 2
        assert chunks[1].startswith("data: ") and chunks[1].endswith("\n\n")
        payload = json.loads(chunks[1][len("data: "):])
        assert set(payload) == {"AAPL", "GOOGL"}
        assert payload["AAPL"]["price"] == 190.0
        assert payload["AAPL"]["ticker"] == "AAPL"

    async def test_empty_cache_emits_no_data(self):
        chunks = await _collect(PriceCache(), FakeRequest(3))
        assert chunks == ["retry: 1000\n\n"]

    async def test_unchanged_version_not_resent(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        chunks = await _collect(cache, FakeRequest(4))
        assert sum(c.startswith("data:") for c in chunks) == 1

    async def test_new_update_is_sent(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        seen = []
        request = FakeRequest(50)
        async for chunk in _generate_events(cache, request, interval=0.001):
            if chunk.startswith("data:"):
                seen.append(json.loads(chunk[6:]))
                if len(seen) == 1:
                    cache.update("AAPL", 191.0)
                else:
                    break
        assert seen[0]["AAPL"]["price"] == 190.0
        assert seen[1]["AAPL"]["price"] == 191.0
        assert seen[1]["AAPL"]["direction"] == "up"

    async def test_missing_client_handled(self):
        chunks = await _collect(PriceCache(), FakeRequest(0, client=None))
        assert chunks == ["retry: 1000\n\n"]

    async def test_cancellation_is_swallowed(self):
        cache = PriceCache()
        gen = _generate_events(cache, FakeRequest(1000), interval=10)
        assert await gen.__anext__() == "retry: 1000\n\n"
        task = asyncio.ensure_future(gen.__anext__())
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, StopAsyncIteration):
            pass


class TestRouter:
    def test_route_registered(self):
        app = FastAPI()
        app.include_router(create_stream_router(PriceCache()))
        assert "/api/stream/prices" in app.openapi()["paths"]

    async def test_endpoint_returns_sse_response(self):
        cache = PriceCache()
        router = create_stream_router(cache)
        route = next(r for r in router.routes if r.path == "/api/stream/prices")
        response = await route.endpoint(FakeRequest(0))
        assert response.media_type == "text/event-stream"
        assert response.headers["cache-control"] == "no-cache"
        assert response.headers["x-accel-buffering"] == "no"
