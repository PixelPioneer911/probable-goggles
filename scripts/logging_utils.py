import logging
import os
from pathlib import Path
from typing import Iterable


def configure_logging(logger_name: str) -> logging.Logger:
    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    telethon_level_name = os.environ.get("TELETHON_LOG_LEVEL", "WARNING").upper()
    telethon_level = getattr(logging, telethon_level_name, logging.WARNING)

    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    else:
        root_logger.setLevel(level)

    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logging.getLogger("telethon").setLevel(telethon_level)
    return logger


def format_path(path: Path) -> str:
    try:
        return str(path.relative_to(Path.cwd()))
    except ValueError:
        return str(path)


def summarize_items(items: Iterable[str], *, limit: int = 5) -> str:
    values = [item for item in items if item]
    if not values:
        return "none"

    preview = values[:limit]
    suffix = "" if len(values) <= limit else f" ... (+{len(values) - limit} more)"
    return ", ".join(preview) + suffix
