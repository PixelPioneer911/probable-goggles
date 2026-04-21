import json
from logging_utils import configure_logging, format_path
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logger = configure_logging(__name__)

url = "https://tsrct.sc/?modal=hide"
html = requests.get(url, timeout=20).text
soup = BeautifulSoup(html, "html.parser")

links = []
for a in soup.select("a[href^='https://t.me/'], a[href^='http://t.me/']"):
    links.append(a["href"])

unique_links = sorted(set(links))
captured_at = datetime.now(timezone.utc)

output = {
    "timestamp": captured_at.isoformat(),
    "source": url,
    "links": unique_links,
}

timestamp = captured_at.strftime("%Y%m%dT%H%M%SZ")
output_path = Path("outputs/fetched_links") / f"fetched_bots_{timestamp}.json"
output_path.parent.mkdir(parents=True, exist_ok=True)
output_path.write_text(json.dumps(output, indent=2), encoding="utf-8")

logger.info("Fetched %d unique Telegram links from %s", len(unique_links), url)
logger.info("Wrote fetch results to %s", format_path(output_path))
