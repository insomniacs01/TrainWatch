import logging

DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def normalize_log_level(value: str) -> int:
    candidate = str(value or "INFO").strip().upper()
    return getattr(logging, candidate, logging.INFO)


def configure_logging(level_name: str = "INFO") -> None:
    logging.basicConfig(
        level=normalize_log_level(level_name),
        format=DEFAULT_LOG_FORMAT,
        force=True,
    )
