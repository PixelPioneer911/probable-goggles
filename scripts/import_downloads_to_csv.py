import argparse
import csv
import json
import os
import re
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import requests

from logging_utils import configure_logging, format_path

logger = configure_logging(__name__)

DOWNLOADS_DIR = Path("outputs/downloads")
OUTPUT_CSV = Path("outputs/normalized/downloads_flat.csv")
CATEGORY_CACHE_PATH = Path("outputs/normalized/category_normalization_cache.json")
DEFAULT_OPENAI_MODEL = "gpt-4.1-mini"
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
OPENAI_MAX_RETRIES = 4

HEADER_RE = re.compile(r"^Price list of @(?P<bot>[^\s#]+)\s+#(?P<timestamp>\S+)$")
PRICE_RE = re.compile(
    r"^from\s+(?P<quantity>[\d,]+(?:\.\d+)?)\s+"
    r"(?P<unit>.+?)\s+@\s+"
    r"(?P<currency>[^\d\s])(?P<price>[\d,]+(?:\.\d+)?)$"
)
STOCK_RE = re.compile(r"^(?P<category>.+?)\s+•\s+Stock\s+(?P<stock>.+)$")


@dataclass(frozen=True)
class ParsedPriceRow:
    source_file: str
    vendor_name: str
    bot_handle: str
    captured_at: str
    item_name: str
    category_path: str
    category_root: str
    category_leaf: str
    normalized_category_path: str
    normalized_category_root: str
    normalized_category_leaf: str
    normalization_reason: str
    stock_text: str
    stock_quantity: str
    quantity_from: str
    unit: str
    currency: str
    price_per_unit: str


@dataclass(frozen=True)
class NormalizedCategory:
    raw_category_path: str
    normalized_category_root: str
    normalized_category_leaf: str
    normalization_reason: str
    source: str

    @property
    def normalized_category_path(self) -> str:
        if self.normalized_category_root and self.normalized_category_leaf:
            return f"{self.normalized_category_root} → {self.normalized_category_leaf}"
        return self.normalized_category_root or self.normalized_category_leaf


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Flatten downloaded bot price lists to a CSV and normalize category names."
    )
    parser.add_argument(
        "--downloads-dir",
        type=Path,
        default=DOWNLOADS_DIR,
        help=f"Directory containing downloaded .txt price lists. Default: {DOWNLOADS_DIR}",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=OUTPUT_CSV,
        help=f"Destination CSV path. Default: {OUTPUT_CSV}",
    )
    parser.add_argument(
        "--category-cache",
        type=Path,
        default=CATEGORY_CACHE_PATH,
        help=f"JSON cache for category normalization results. Default: {CATEGORY_CACHE_PATH}",
    )
    parser.add_argument(
        "--openai-model",
        default=os.environ.get("OPENAI_MODEL", DEFAULT_OPENAI_MODEL),
        help=f"OpenAI model used for category normalization. Default: {DEFAULT_OPENAI_MODEL}",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=25,
        help="How many unique category paths to normalize per OpenAI request. Default: 25",
    )
    parser.add_argument(
        "--skip-openai-normalization",
        action="store_true",
        help="Write the CSV without calling OpenAI; normalized columns will mirror the raw category fields.",
    )
    return parser.parse_args()


def parse_decimal(text: str) -> str:
    return text.replace(",", "")


def split_category(category_path: str) -> tuple[str, str]:
    parts = [part.strip() for part in category_path.split("→") if part.strip()]
    if not parts:
        return "", ""
    return parts[0], parts[-1]


def infer_vendor_name(file_path: Path, bot_handle: str) -> str:
    stem = file_path.stem
    suffix_index = stem.lower().find("_bot-listings-")
    if suffix_index == -1:
        suffix_index = stem.lower().find("_tsbot-listings-")
    if suffix_index == -1:
        suffix_index = stem.lower().find("_ts_bot-listings-")
    if suffix_index == -1:
        return bot_handle
    return stem[:suffix_index]


def parse_header(lines: list[str], file_path: Path) -> tuple[str, str]:
    if not lines:
        raise ValueError(f"{file_path} is empty")

    match = HEADER_RE.match(lines[0].strip())
    if not match:
        raise ValueError(f"Unexpected header format in {file_path}: {lines[0]!r}")

    return match.group("bot"), match.group("timestamp")


def load_category_cache(cache_path: Path) -> dict[str, NormalizedCategory]:
    if not cache_path.exists():
        return {}

    raw_cache = json.loads(cache_path.read_text(encoding="utf-8"))
    cache: dict[str, NormalizedCategory] = {}
    for raw_category_path, payload in raw_cache.items():
        cache[raw_category_path] = NormalizedCategory(
            raw_category_path=raw_category_path,
            normalized_category_root=payload.get("normalized_category_root", "").strip(),
            normalized_category_leaf=payload.get("normalized_category_leaf", "").strip(),
            normalization_reason=payload.get("normalization_reason", "").strip(),
            source=payload.get("source", "fallback").strip() or "fallback",
        )
    return cache


def save_category_cache(cache_path: Path, cache: dict[str, NormalizedCategory]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        raw_category_path: {
            "normalized_category_root": entry.normalized_category_root,
            "normalized_category_leaf": entry.normalized_category_leaf,
            "normalization_reason": entry.normalization_reason,
            "source": entry.source,
        }
        for raw_category_path, entry in sorted(cache.items())
        if entry.source == "openai"
    }
    cache_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def fallback_normalization(raw_category_path: str) -> NormalizedCategory:
    category_root, category_leaf = split_category(raw_category_path)
    return NormalizedCategory(
        raw_category_path=raw_category_path,
        normalized_category_root=category_root,
        normalized_category_leaf=category_leaf,
        normalization_reason="Used raw category because OpenAI normalization was skipped or unavailable.",
        source="fallback",
    )


def normalize_model_output_item(item: dict[str, Any]) -> NormalizedCategory:
    raw_category_path = str(item.get("raw_category_path", "")).strip()
    normalized_category_root = str(item.get("normalized_category_root", "")).strip()
    normalized_category_leaf = str(item.get("normalized_category_leaf", "")).strip()
    normalization_reason = str(item.get("normalization_reason", "")).strip()

    if not raw_category_path:
        raise ValueError("Model response is missing raw_category_path")
    if not normalized_category_root:
        raise ValueError(f"Model response is missing normalized_category_root for {raw_category_path!r}")
    if not normalized_category_leaf:
        raise ValueError(f"Model response is missing normalized_category_leaf for {raw_category_path!r}")

    return NormalizedCategory(
        raw_category_path=raw_category_path,
        normalized_category_root=normalized_category_root,
        normalized_category_leaf=normalized_category_leaf,
        normalization_reason=normalization_reason or "Normalized by OpenAI.",
        source="openai",
    )


def extract_response_text(payload: dict[str, Any]) -> str:
    output_items = payload.get("output", [])
    text_chunks: list[str] = []

    for output_item in output_items:
        for content_item in output_item.get("content", []):
            if content_item.get("type") == "output_text":
                text_value = content_item.get("text", "")
                if text_value:
                    text_chunks.append(text_value)

    response_text = "".join(text_chunks).strip()
    if response_text:
        return response_text

    refusal = payload.get("refusal")
    if refusal:
        raise ValueError(f"OpenAI refused the normalization request: {refusal}")

    raise ValueError("OpenAI response did not include structured text output")


def call_openai_category_normalizer(
    raw_category_paths: list[str],
    *,
    api_key: str,
    model: str,
) -> dict[str, NormalizedCategory]:
    schema = {
        "type": "object",
        "properties": {
            "categories": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "raw_category_path": {"type": "string"},
                        "normalized_category_root": {"type": "string"},
                        "normalized_category_leaf": {"type": "string"},
                        "normalization_reason": {"type": "string"},
                    },
                    "required": [
                        "raw_category_path",
                        "normalized_category_root",
                        "normalized_category_leaf",
                        "normalization_reason",
                    ],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["categories"],
        "additionalProperties": False,
    }

    system_prompt = (
        "You normalize marketplace category labels into a consistent taxonomy for spreadsheet analysis. "
        "Return JSON only. Keep the output conservative and based only on the category label text. "
        "Use short canonical labels in title case. The normalized root should be broad, such as "
        "Cannabis, Stimulants, Opiates, Benzodiazepines, Dissociatives, Empathogens, Psychedelics, "
        "Pharmaceuticals, Combinations, or Other. The normalized leaf should be a stable subcategory such as "
        "Cocaine, Crack Cocaine, Amphetamine, Ketamine, Heroin, Pills, MDMA, UK Grown, Edibles, Vapes, or Other. "
        "If the label is already consistent, keep it. Do not invent details not present in the label."
    )
    user_prompt = (
        "Normalize these raw category paths:\n"
        + "\n".join(f"- {value}" for value in raw_category_paths)
    )

    request_payload = {
        "model": model,
        "input": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "category_normalizations",
                "strict": True,
                "schema": schema,
            }
        },
    }

    last_error: Exception | None = None
    for attempt in range(1, OPENAI_MAX_RETRIES + 1):
        response = requests.post(
            OPENAI_RESPONSES_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=request_payload,
            timeout=90,
        )

        if response.status_code < 400:
            payload = response.json()
            response_text = extract_response_text(payload)
            parsed = json.loads(response_text)
            items = parsed.get("categories", [])
            normalized = {
                entry.raw_category_path: entry
                for entry in (normalize_model_output_item(item) for item in items)
            }

            missing = sorted(set(raw_category_paths) - set(normalized))
            if missing:
                raise ValueError(f"OpenAI response omitted {len(missing)} categories: {missing[:5]}")

            return normalized

        error_body = response.text.strip()
        if response.status_code in {429, 500, 502, 503, 504} and attempt < OPENAI_MAX_RETRIES:
            wait_seconds = min(2 ** (attempt - 1), 8)
            logger.warning(
                "OpenAI request failed with HTTP %s on attempt %s/%s. Retrying in %ss. Response: %s",
                response.status_code,
                attempt,
                OPENAI_MAX_RETRIES,
                wait_seconds,
                error_body[:400],
            )
            time.sleep(wait_seconds)
            continue

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            last_error = requests.HTTPError(
                f"{exc}. Response body: {error_body[:1200]}",
                response=response,
            )
            break

    if last_error is not None:
        raise last_error

    raise RuntimeError("OpenAI category normalization failed without returning a response")


def chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def get_category_normalizations(
    raw_category_paths: set[str],
    *,
    cache_path: Path,
    model: str,
    batch_size: int,
    skip_openai_normalization: bool,
) -> dict[str, NormalizedCategory]:
    cache = load_category_cache(cache_path)
    finalised = {raw_category_path for raw_category_path, entry in cache.items() if entry.source == "openai"}
    missing = sorted(raw_category_paths - finalised)

    if not missing:
        logger.info("Loaded %d cached category normalization(s)", len(cache))
        return cache

    if skip_openai_normalization:
        logger.info("Skipping OpenAI normalization for %d uncached category path(s)", len(missing))
        return cache

    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        logger.warning(
            "OPENAI_API_KEY is not set, so uncached categories will keep their raw labels."
        )
        return cache

    for batch in chunked(missing, max(1, batch_size)):
        logger.info(
            "Normalizing %d category path(s) with OpenAI model %s",
            len(batch),
            model,
        )
        try:
            normalized_batch = call_openai_category_normalizer(batch, api_key=api_key, model=model)
        except Exception as exc:
            logger.warning(
                "OpenAI normalization failed for a batch of %d category path(s): %s. "
                "Falling back to raw labels for this batch.",
                len(batch),
                exc,
            )
            normalized_batch = {
                raw_category_path: fallback_normalization(raw_category_path)
                for raw_category_path in batch
            }
        cache.update(normalized_batch)
        save_category_cache(cache_path, cache)

    return cache


def parse_entry(
    entry_text: str,
    *,
    source_file: str,
    vendor_name: str,
    bot_handle: str,
    captured_at: str,
    category_normalizations: dict[str, NormalizedCategory],
) -> list[ParsedPriceRow]:
    lines = [line.strip() for line in entry_text.splitlines() if line.strip()]
    if len(lines) < 3:
        return []

    item_name = lines[0]
    stock_match = STOCK_RE.match(lines[1])
    if not stock_match:
        logger.warning("Skipping block with unrecognised category line: %r", lines[1])
        return []

    category_path = stock_match.group("category").strip()
    stock_text = stock_match.group("stock").strip()
    category_root, category_leaf = split_category(category_path)
    normalized_category = category_normalizations.get(category_path, fallback_normalization(category_path))

    stock_quantity = ""
    if stock_text.lower() != "unlimited":
        stock_quantity = parse_decimal(stock_text)

    rows: list[ParsedPriceRow] = []
    for price_line in lines[2:]:
        match = PRICE_RE.match(price_line)
        if not match:
            logger.warning("Skipping unrecognised price line in %s: %r", source_file, price_line)
            continue

        rows.append(
            ParsedPriceRow(
                source_file=source_file,
                vendor_name=vendor_name,
                bot_handle=bot_handle,
                captured_at=captured_at,
                item_name=item_name,
                category_path=category_path,
                category_root=category_root,
                category_leaf=category_leaf,
                normalized_category_path=normalized_category.normalized_category_path,
                normalized_category_root=normalized_category.normalized_category_root,
                normalized_category_leaf=normalized_category.normalized_category_leaf,
                normalization_reason=normalized_category.normalization_reason,
                stock_text=stock_text,
                stock_quantity=stock_quantity,
                quantity_from=parse_decimal(match.group("quantity")),
                unit=match.group("unit").strip(),
                currency=match.group("currency"),
                price_per_unit=parse_decimal(match.group("price")),
            )
        )

    return rows


def parse_download(
    file_path: Path,
    *,
    category_normalizations: dict[str, NormalizedCategory],
) -> list[ParsedPriceRow]:
    lines = file_path.read_text(encoding="utf-8").splitlines()
    bot_handle, captured_at = parse_header(lines, file_path)
    vendor_name = infer_vendor_name(file_path, bot_handle)

    body = "\n".join(lines[2:]).strip()
    if not body:
        return []

    entries = [block.strip() for block in body.split("-------------") if block.strip()]
    rows: list[ParsedPriceRow] = []
    for entry in entries:
        rows.extend(
            parse_entry(
                entry,
                source_file=file_path.name,
                vendor_name=vendor_name,
                bot_handle=bot_handle,
                captured_at=captured_at,
                category_normalizations=category_normalizations,
            )
        )
    return rows


def collect_raw_category_paths(files: list[Path]) -> set[str]:
    raw_category_paths: set[str] = set()
    for file_path in files:
        lines = file_path.read_text(encoding="utf-8").splitlines()
        body = "\n".join(lines[2:]).strip()
        if not body:
            continue

        entries = [block.strip() for block in body.split("-------------") if block.strip()]
        for entry in entries:
            entry_lines = [line.strip() for line in entry.splitlines() if line.strip()]
            if len(entry_lines) < 2:
                continue

            stock_match = STOCK_RE.match(entry_lines[1])
            if stock_match:
                raw_category_paths.add(stock_match.group("category").strip())
    return raw_category_paths


def write_csv(rows: list[ParsedPriceRow], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(ParsedPriceRow.__dataclass_fields__.keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def main() -> None:
    load_dotenv(Path(".env"))
    args = parse_args()
    files = sorted(args.downloads_dir.glob("*.txt"))
    if not files:
        raise FileNotFoundError(f"No .txt files found in {args.downloads_dir}")

    raw_category_paths = collect_raw_category_paths(files)
    logger.info("Discovered %d unique raw category path(s)", len(raw_category_paths))
    category_normalizations = get_category_normalizations(
        raw_category_paths,
        cache_path=args.category_cache,
        model=args.openai_model,
        batch_size=args.batch_size,
        skip_openai_normalization=args.skip_openai_normalization,
    )

    all_rows: list[ParsedPriceRow] = []
    for file_path in files:
        parsed_rows = parse_download(
            file_path,
            category_normalizations=category_normalizations,
        )
        logger.info("Parsed %d row(s) from %s", len(parsed_rows), format_path(file_path))
        all_rows.extend(parsed_rows)

    write_csv(all_rows, args.output_csv)
    logger.info("Wrote %d row(s) to %s", len(all_rows), format_path(args.output_csv))
    logger.info("Category normalization cache is at %s", format_path(args.category_cache))


if __name__ == "__main__":
    main()
