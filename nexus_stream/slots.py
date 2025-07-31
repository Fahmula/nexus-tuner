import asyncio
import threading
import os
import sys
from typing import Any, Final, Literal

from nexus_stream.utils import M3UURL, ActiveStreams, AvailableStreams, MaxStreams, ProviderAlias


class CountingSemaphore(asyncio.Semaphore):
    def __init__(self, value: int, total_slots: MaxStreams) -> None:
        super().__init__(value)
        self._total_slots: MaxStreams = total_slots

    async def acquire(self) -> str:  # type: ignore[reportIncompatibleMethodOverride]
        """Acquires the semaphore and returns the new number of active slots."""
        await super().acquire()
        if self._value < 0:
            if threading.current_thread() is threading.main_thread():
                sys.exit(7)
            else:
                os._exit(7)
        return f"{self._total_slots - self._value}/{self._total_slots}"

    def release(self) -> str:  # type: ignore[reportIncompatibleMethodOverride]
        """Releases the semaphore and returns the new number of active slots."""
        super().release()
        if self._value > self._total_slots:
            if threading.current_thread() is threading.main_thread():
                sys.exit(13)
            else:
                os._exit(13)
        return f"{self._total_slots - self._value}/{self._total_slots}"


class ProviderSlots:
    """
    An asyncio-native class to represent a provider with its associated slots.
    Uses a custom CountingSemaphore to enable accurate concurrent logging.
    """
    __slots__ = ('_alias', '_m3u_url', '_total_slots', '_lock', '_semaphore', '_background_tasks', '_cancelled_tasks')

    def __init__(self, alias: ProviderAlias, m3u_url: M3UURL, total_slots: MaxStreams) -> None:
        self._alias: ProviderAlias = alias
        self._m3u_url: M3UURL = m3u_url
        self._total_slots: MaxStreams = total_slots
        self._lock: asyncio.Lock = asyncio.Lock()
        self._semaphore: CountingSemaphore = CountingSemaphore(total_slots, total_slots)
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._cancelled_tasks: set[asyncio.Task[Any]] = set()

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

    def add_background_task(self, task: asyncio.Task[Any]) -> None:
        """Add a background task to the provider slots."""
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def cancel_background_tasks(self) -> None:
        """Cancel all background tasks associated with the provider slots."""
        for task in self._background_tasks:
            if not task.done():
                task.cancel()
                self._cancelled_tasks.add(task)

    def pop_cancelled_task(self, task: asyncio.Task[Any]) -> bool:
        """Remove a task from the cancelled tasks set if it exists."""
        if task in self._cancelled_tasks:
            self._cancelled_tasks.remove(task)
            return True
        return False

    async def try_acquire(self) -> str | Literal[False]:
        """Attempts to acquire a slot, be sure to check if total_slots is greater than 0
        for this provider before running any tasks that uses slots.
        """
        async with self._lock:
            initial = self._semaphore._value
            try:
                return await asyncio.wait_for(self._semaphore.acquire(), timeout=0.1)
            except BaseException as e:
                if self._semaphore._value != initial:
                    self._semaphore.release()
                if isinstance(e, asyncio.TimeoutError):
                    return False
                raise e

    async def release(self) -> str:
        """Cancel the release. WARNING: This method must be called with run_bg() or
        in a context where cancellation is not possibe.
        """
        async with self._lock:
            return self._semaphore.release()
