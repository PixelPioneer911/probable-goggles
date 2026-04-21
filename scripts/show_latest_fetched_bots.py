import json
from pathlib import Path

from logging_utils import configure_logging, format_path, summarize_items

logger = configure_logging(__name__)

fetch_dir = Path("outputs/fetched_links")
files = sorted(fetch_dir.glob("fetched_bots_*.json"))

if not files:
    raise FileNotFoundError(f"No fetch files found in {fetch_dir}")

latest_file = files[-1]
data = json.loads(latest_file.read_text(encoding="utf-8"))
links = data.get("links", [])

logger.info("Latest fetch file: %s", format_path(latest_file))
logger.info("Captured at: %s", data.get("timestamp", "unknown"))
logger.info("Source: %s", data.get("source", "unknown"))
logger.info("Discovered %d Telegram links", len(links))
logger.info("Link preview: %s", summarize_items(links, limit=8))
