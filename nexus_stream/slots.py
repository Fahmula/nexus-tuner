import threading
from time import monotonic
from typing import NewType


ProviderName = NewType("ProviderName", str)


class ProviderSlots:
    """
    A class to represent a provider with its associated slots.
    Based on the implementation of threading.Semaphore.
    """

    def __init__(self, name: ProviderName, m3u_url: str, total_slots: int) -> None:
        if total_slots < 1:
            raise ValueError("total_slots must be >= 1")
        self._name = name
        self._m3u_url = m3u_url
        self._total_slots = total_slots
        self._available_slots = total_slots
        self._active_slots = 0
        self._cond = threading.Condition(threading.Lock())

    def __repr__(self) -> str:
        return f"Provider(name={self._name}, total_slots={self._available_slots}, active_slots={self._active_slots})"

    def get_name(self) -> ProviderName:
        return self._name

    def get_m3u_url(self) -> str:
        return self._m3u_url

    def get_total_slots(self) -> int:
        return self._total_slots

    def get_available_slots(self) -> int:
        return self._available_slots

    def get_active_slots(self) -> int:
        return self._active_slots

    def acquire(self, blocking: bool = True, timeout: float | None = None) -> bool:
        if not blocking and timeout is not None:
            raise ValueError("can't specify timeout for non-blocking acquire")
        rc = False
        endtime: float | None = None
        with self._cond:
            while self._available_slots == 0:
                if not blocking:
                    break
                if timeout is not None:
                    if endtime is None:
                        endtime = monotonic() + timeout
                    else:
                        timeout = endtime - monotonic()
                        if timeout <= 0:
                            break
                self._cond.wait(timeout)
            else:
                self._available_slots -= 1
                self._active_slots += 1
                rc = True
        return rc

    def release(self) -> None:
        with self._cond:
            self._available_slots += 1
            self._active_slots -= 1
            self._cond.notify(1)

    __enter__ = acquire

    def __exit__(self, _: object, __: object, ___: object) -> None:
        self.release()

