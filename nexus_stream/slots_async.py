import asyncio
from time import monotonic
from typing import NewType

ProviderName = NewType("ProviderName", str)


# Refactor Note: This custom semaphore now returns the new active count upon acquisition.
# This makes the acquisition and state-checking an atomic operation for logging purposes.
class CountingSemaphore(asyncio.Semaphore):
    def __init__(self, value: int, total_slots: int) -> None:
        super().__init__(value)
        self._total_slots = total_slots

    async def acquire(self) -> int:
        """Acquires the semaphore and returns the new number of active slots."""
        await super().acquire()
        # The internal `_value` is the number of available slots.
        # Active slots = Total - Available.
        return self._total_slots - self._value


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
        # Refactor Note: Using the new CountingSemaphore to get accurate post-acquisition state.
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

    # Refactor Note: The acquire method now returns the new active slot count.
    async def acquire(self) -> int:
        """Acquires a slot and returns the new total number of active slots."""
        return await self._semaphore.acquire()

    async def release(self) -> None:
        """Releases a slot, making it available for other coroutines."""
        self._semaphore.release()

    # Note: The async context manager (`async with`) does not return the count.
    # For accurate logging, direct `await .acquire()` should be used.
    async def __aenter__(self) -> "ProviderSlots":
        """Acquires a slot for use in an 'async with' block."""
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Ensures a slot is released when exiting an 'async with' block."""
        await self.release()