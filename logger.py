"""logger.py – Application-wide logging utility.

Provides:
- ``Logger``: A class wrapping Python's standard ``logging`` module with
  optional file output and Telegram notification support.
- ``with_logging``: A decorator factory that logs START / END / ERROR for
  any wrapped function.

Usage::

    import logger
    log = logger.Logger("my_module")
    log.msg("Hello")

    @logger.with_logging(log)
    def my_func():
        ...
"""
import logging
import functools
from typing import Callable, Any, Optional

from notify_telegram import simpleTelegram


class Logger:
    """Thin wrapper around Python's ``logging`` module.

    Args:
        fn: Base name used for both the logger name and the log file
            (``log_<fn>``).
        noty: Optional Telegram notifier for push messages.
        to_file: Write log records to ``log_<fn>`` when ``True`` (default).
    """

    def __init__(
        self,
        fn: str,
        noty: Optional[simpleTelegram] = None,
        to_file: bool = True,
    ) -> None:
        self.fn   = fn
        self.noty = noty

        self._log = logging.getLogger(fn)
        self._log.setLevel(logging.DEBUG)
        self._log.propagate = False

        # Avoid adding duplicate handlers if the logger is re-used
        if not self._log.handlers and to_file:
            formatter   = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            file_handler = logging.FileHandler(f"log_{fn}", mode="w", encoding="utf-8")
            file_handler.setFormatter(formatter)
            self._log.addHandler(file_handler)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def msg(self, message: str, *, send: bool = False) -> None:
        """Log *message* at INFO level, print to stdout, and optionally
        send a Telegram push notification.

        Args:
            message: Text to log.
            send: If ``True`` and a :class:`simpleTelegram` notifier was
                provided, send the message via Telegram.
        """
        print(message)
        self._log.info(message)
        if send and self.noty is not None:
            self.noty.sendMsg(message)

    def warning(self, message: str) -> None:
        """Log *message* at WARNING level."""
        print(f"[WARNING] {message}")
        self._log.warning(message)

    def error(self, message: str, exc: Optional[Exception] = None) -> None:
        """Log *message* at ERROR level, optionally including exception info.

        Args:
            message: Description of the error.
            exc: Exception instance to attach (adds traceback to the log).
        """
        print(f"[ERROR] {message}")
        if exc is not None:
            self._log.error(message, exc_info=exc)
        else:
            self._log.error(message)

    def __repr__(self) -> str:  # pragma: no cover
        return f"Logger(fn={self.fn!r}, handlers={self._log.handlers!r})"


# ---------------------------------------------------------------------------
# Backward-compatibility alias
# Existing call-sites that use ``logger.logger(...)`` continue to work.
# ---------------------------------------------------------------------------
logger = Logger  # noqa: N816


# ---------------------------------------------------------------------------
# Decorator
# ---------------------------------------------------------------------------

def with_logging(log: Logger) -> Callable:
    """Decorator factory: log START / END / ERROR around a function call.

    The wrapped function should signal an error by returning
    ``("ERROR", exception_instance)``.  Any other return value is treated as
    success.

    Args:
        log: A :class:`Logger` instance to write to.

    Example::

        @with_logging(log)
        def fetch_data() -> dict | tuple[str, Exception]:
            ...
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # log.msg(f"{func.__name__} : START")
            value = func(*args, **kwargs)
            if isinstance(value, tuple) and value and value[0] == "ERROR":
                log.error(f"{func.__name__} : ERROR", exc=value[1] if len(value) > 1 else None)
            # else:
            #     log.msg(f"{func.__name__} : END")
            return value
        return wrapper
    return decorator
