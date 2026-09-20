"""An absolute network budget shared by redirects, retries and body readers.

Socket inactivity timeouts do not stop a peer trickling bytes: each byte
resets the clock, so a request can outlive any per-read timeout. Guarded
requests register their live sockets here and a single watchdog thread shuts
those sockets down when the budget expires, which unblocks even a thread
parked in ``recv``. The watchdog is cancelled and joined before its scope
exits; no abandoned download thread survives a timed-out caller.
"""
from __future__ import annotations

import socket
import threading
import time
import weakref
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

#: The watchdog thread's name — one per active scope, and none once it closes.
WATCHDOG_THREAD = "http-deadline-watchdog"


class HttpDeadlineExceededError(TimeoutError):
    """The absolute network budget has expired."""


class _Deadline:
    """One monotonic budget and the sockets it may interrupt."""

    def __init__(self, until: float):
        self.until = until
        self._lock = threading.Lock()
        self._fired = False
        # Weak: a socket the transport has finished with must not be kept
        # alive (nor its entry accumulated) by this bookkeeping.
        self._sockets: weakref.WeakSet[socket.socket] = weakref.WeakSet()
        self._timer = threading.Timer(max(0.0, until - time.monotonic()), self._interrupt)
        self._timer.name = WATCHDOG_THREAD
        self._timer.daemon = True
        self._timer.start()

    def remaining(self) -> float:
        remaining = self.until - time.monotonic()
        if remaining <= 0:
            raise HttpDeadlineExceededError("HTTP deadline exceeded")
        return remaining

    def watch(self, sock: socket.socket) -> None:
        """Register a live socket; raises once the budget is gone."""
        with self._lock:
            if self._fired:
                # The watchdog has already run: it will never see this socket,
                # so the caller must not start a read on it.
                raise HttpDeadlineExceededError("HTTP deadline exceeded")
            self.remaining()
            self._sockets.add(sock)

    def release(self, sock: socket.socket) -> None:
        """Forget a socket the transport has closed (belt to the weak set)."""
        with self._lock:
            self._sockets.discard(sock)

    def _interrupt(self) -> None:
        with self._lock:
            self._fired = True
            for sock in list(self._sockets):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    # Already closed or never connected. This is shutdown, not
                    # close: a socket object that was closed reports fileno()
                    # -1 and raises here, so no reused descriptor can be hit.
                    pass

    def close(self) -> None:
        self._timer.cancel()
        self._timer.join()


_ACTIVE: ContextVar[_Deadline | None] = ContextVar("http_deadline", default=None)


@contextmanager
def http_deadline(until: float) -> Iterator[None]:
    """Apply one monotonic deadline to synchronous HTTP work in this scope.

    A nested scope may only TIGHTEN the budget: a longer ``until`` inside an
    active, stricter deadline reuses the one already running.
    """
    existing = _ACTIVE.get()
    if existing is not None and existing.until <= until:
        existing.remaining()
        yield
        return
    deadline = _Deadline(until)
    token = _ACTIVE.set(deadline)
    try:
        yield
        # Work that ignored the budget still reports the timeout.
        deadline.remaining()
    finally:
        deadline.close()
        _ACTIVE.reset(token)


def remaining_timeout(timeout: float | None = None) -> float | None:
    """Cap a timeout by the active budget; raise before starting expired work."""
    active = _ACTIVE.get()
    if active is None:
        return timeout
    remaining = active.remaining()
    return min(timeout, remaining) if timeout is not None else remaining


def deadline_sleep(seconds: float) -> None:
    """Sleep for pacing/backoff without exceeding the active budget."""
    time.sleep(remaining_timeout(seconds))
    remaining_timeout()


def watch_socket(sock: socket.socket | None) -> None:
    """Interrupt this socket when the current budget expires."""
    active = _ACTIVE.get()
    if active is not None and sock is not None:
        active.watch(sock)


def release_socket(sock: socket.socket | None) -> None:
    """Stop watching a socket the transport has closed."""
    active = _ACTIVE.get()
    if active is not None and sock is not None:
        active.release(sock)
