import asyncio
import threading
import os
import sys
from typing import Any, NewType, List

ProviderName = NewType("ProviderName", str)

GRACE_PERIOD = 3

class CountingSemaphore(asyncio.Semaphore):
    def __init__(self, value: int, total_slots: int) -> None:
        super().__init__(value)
        self._total_slots = total_slots

    async def acquire(self) -> str:
        """Acquires the semaphore and returns the new number of active slots."""
        await super().acquire()
        active_slots = self._total_slots - self._value
        if active_slots > self._total_slots:
            if threading.current_thread() is threading.main_thread():
                sys.exit(7)
            else:
                os._exit(7)
        return f"{active_slots}/{self._total_slots}"

    def release(self) -> str:
        """Releases the semaphore and returns the new number of active slots."""
        super().release()
        active_slots = self._total_slots - self._value
        if active_slots < 0:
            if threading.current_thread() is threading.main_thread():
                sys.exit(13)
            else:
                os._exit(13)
        return f"{active_slots}/{self._total_slots}"


class ProviderSlots:
    """
    An asyncio-native class to represent a provider with its associated slots.
    Uses a custom CountingSemaphore to enable accurate concurrent logging.
    """

    def __init__(self, name: ProviderName, m3u_url: str, total_slots: int) -> None:
        if total_slots < 1:
            raise ValueError("total_slots must be >= 1")
        self._name = name
        self._m3u_url = m3u_url
        self._total_slots = total_slots
        self._active_background_tasks: List[asyncio.Task[Any]] = []
        self._lock = asyncio.Lock()
        self._semaphore = CountingSemaphore(total_slots, total_slots)

    def __repr__(self) -> str:
        return (
            f"Provider(name={self._name}, total_slots={self._total_slots}, "
            f"active_slots={self.get_active_slots()})"
        )

    def get_name(self) -> ProviderName:
        return self._name

    def get_m3u_url(self) -> str:
        return self._m3u_url

    def get_total_slots(self) -> int:
        return self._total_slots

    def get_available_slots(self) -> int:
        return self._semaphore._value

    def get_active_slots(self) -> int:
        return self._total_slots - self._semaphore._value

    async def acquire_user_slot(self) -> str:
        """
        Acquires a user slot, preempting a background task if necessary.
        """
        async with self._lock:
            try:
                return await asyncio.wait_for(self._semaphore.acquire(), timeout=0.01)
            except asyncio.TimeoutError:
                pass

            task_to_preempt = None
            if self._active_background_tasks:
                task_to_preempt = self._active_background_tasks[0]

            if task_to_preempt:
                try:
                    await asyncio.wait_for(asyncio.shield(task_to_preempt), timeout=GRACE_PERIOD)
                except asyncio.TimeoutError:
                    task_to_preempt.cancel()
                except asyncio.CancelledError:
                    pass

            try:
                return await asyncio.wait_for(self._semaphore.acquire(), timeout=3.0)
            except asyncio.TimeoutError:
                raise asyncio.TimeoutError(f"Could not acquire preempted slot for {self._name}.")


    async def release_user_slot(self) -> str:
        """Releases a slot for a user."""
        async with self._lock:
            new_active_count = self._semaphore.release()
            return new_active_count

    async def acquire_background_slot(self, task: asyncio.Task[Any]) -> None:
        """ Acquires a slot for a background task and registers the task. """
        async with self._lock:
            try:
                await asyncio.wait_for(self._semaphore.acquire(), timeout=0.01)
                self._active_background_tasks.append(task)
            except asyncio.TimeoutError:
                raise

    async def release_background_slot(self, task: asyncio.Task[Any]) -> None:
        """ Releases a slot for background tasks and de-registers the task. """
        async with self._lock:
            self._semaphore.release()
            if task in self._active_background_tasks:
                self._active_background_tasks.remove(task)
