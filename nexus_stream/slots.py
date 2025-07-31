import asyncio
import threading
import os
import sys
from typing import Final, Literal

from nexus_stream.utils import M3UURL, ActiveStreams, AvailableStreams, MaxStreams, ProviderAlias, run_bg

GRACE_PERIOD: Final[float] = 0.01

class CountingSemaphore(asyncio.Semaphore):
    def __init__(self, value: int, total_slots: MaxStreams) -> None:
        super().__init__(value)
        self._total_slots: MaxStreams = total_slots

    async def acquire(self) -> Literal[True]:
        """Acquires the semaphore and returns the new number of active slots."""
        await super().acquire()
        if self._value < 0:
            if threading.current_thread() is threading.main_thread():
                sys.exit(7)
            else:
                os._exit(7)
        return True

    def release(self) -> None:
        """Releases the semaphore and returns the new number of active slots."""
        super().release()
        if self._value > self._total_slots:
            if threading.current_thread() is threading.main_thread():
                sys.exit(13)
            else:
                os._exit(13)


class ProviderSlots:
    """
    An asyncio-native class to represent a provider with its associated slots.
    Uses a custom CountingSemaphore to enable accurate concurrent logging.
    """
    __slots__ = ('_alias', '_m3u_url', '_total_slots', '_lock', '_semaphore')

    def __init__(self, alias: ProviderAlias, m3u_url: M3UURL, total_slots: MaxStreams) -> None:
        if total_slots < 1:
            raise ValueError("total_slots must be >= 1")
        self._alias: ProviderAlias = alias
        self._m3u_url: M3UURL = m3u_url
        self._total_slots: MaxStreams = total_slots
        self._lock: asyncio.Lock = asyncio.Lock()
        self._semaphore: CountingSemaphore = CountingSemaphore(total_slots, total_slots)

    def get_alias(self) -> ProviderAlias:
        return self._alias

    def get_m3u_url(self) -> M3UURL:
        return self._m3u_url

    def get_total_slots(self) -> MaxStreams:
        return self._total_slots

    async def get_available_slots(self) -> AvailableStreams:
        async with self._lock:
            return AvailableStreams(self._semaphore._value)

    async def get_active_slots(self) -> ActiveStreams:
        async with self._lock:
            return ActiveStreams(self._total_slots - self._semaphore._value)

    async def get_status(self) -> str:
        async with self._lock:
            return f"{self._total_slots - self._semaphore._value}/{self._total_slots}"

    async def try_acquire(self) -> bool:
        """Attempts to acquire a slot."""
        async with self._lock:
            initial = self._semaphore._value
            try:
                return await asyncio.wait_for(self._semaphore.acquire(), timeout=GRACE_PERIOD)
            except BaseException as e:
                if self._semaphore._value != initial:
                    self._semaphore.release()
                if isinstance(e, asyncio.TimeoutError):
                    return False
                raise e

    async def _release(self) -> None:
        async with self._lock:
            self._semaphore.release()

    def release(self) -> None:
        """Cancel the release"""
        run_bg(self._release())
