import argparse
import asyncio
import json
import os
import random
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from logging_utils import configure_logging, format_path, summarize_items
from telegram_bot_flow import BotFlowConfig, run_bot_flow

logger = configure_logging(__name__)


@dataclass(frozen=True)
class BatchDownloadResult:
    link: str
    bot_username: str
    success: bool
    download_path: str | None = None
    error: str | None = None


def load_latest_fetch(fetch_dir: Path) -> tuple[Path, dict]:
    files = sorted(fetch_dir.glob("fetched_bots_*.json"))
    if not files:
        raise FileNotFoundError(f"No fetch files found in {fetch_dir}")

    latest_file = files[-1]
    data = json.loads(latest_file.read_text(encoding="utf-8"))
    return latest_file, data


def extract_bot_username(link: str) -> str:
    parsed = urlparse(link)
    username = parsed.path.strip("/").split("/", 1)[0]
    if not username:
        raise ValueError(f"Could not extract bot username from link: {link}")
    return username if username.startswith("@") else f"@{username}"


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download price lists for all bots in the latest fetch output.",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry only the failed links from the most recent batch download summary.",
    )
    return parser


def get_retry_failed_path(reports_dir: Path) -> Path:
    return reports_dir / "latest_failed_links.txt"


def load_links_for_run(fetch_dir: Path, reports_dir: Path, retry_failed: bool) -> tuple[str, Path | None, list[str]]:
    if retry_failed:
        failed_links_path = get_retry_failed_path(reports_dir)
        if not failed_links_path.exists():
            logger.warning(
                "No failed-links retry file found at %s; falling back to the latest fetch",
                format_path(failed_links_path),
            )
            latest_file, data = load_latest_fetch(fetch_dir)
            return "latest-fetch", latest_file, data.get("links", [])

        links = [
            line.strip()
            for line in failed_links_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        if not links:
            logger.warning(
                "Retry file %s is empty; there are no failed links to retry",
                format_path(failed_links_path),
            )
        return "retry-failed", failed_links_path, links

    latest_file, data = load_latest_fetch(fetch_dir)
    return "latest-fetch", latest_file, data.get("links", [])


async def randomized_batch_delay() -> None:
    lower = float(os.environ.get("TG_MIN_BOT_DELAY", "1.5"))
    upper = float(os.environ.get("TG_MAX_BOT_DELAY", "4.0"))
    delay_seconds = random.uniform(min(lower, upper), max(lower, upper))
    logger.debug("Waiting %.2fs before the next bot", delay_seconds)
    await asyncio.sleep(delay_seconds)


def create_result(link: str, success: bool, *, download_path: Path | None = None, error: str | None = None) -> BatchDownloadResult:
    try:
        bot_username = extract_bot_username(link)
    except Exception:
        bot_username = "unknown"

    normalized_download_path = format_path(download_path) if download_path else None
    return BatchDownloadResult(
        link=link,
        bot_username=bot_username,
        success=success,
        download_path=normalized_download_path,
        error=error,
    )


async def process_links(links: list[str], config: BotFlowConfig) -> list[BatchDownloadResult]:
    if not links:
        raise ValueError("No Telegram links were available to download")

    results: list[BatchDownloadResult] = []
    total = len(links)

    logger.info("Preparing to process %d bot link(s)", total)
    logger.info("Link preview: %s", summarize_items(links, limit=5))

    for index, link in enumerate(links, start=1):
        try:
            bot_username = extract_bot_username(link)
        except Exception as exc:
            logger.warning("Skipping invalid Telegram link %s: %s", link, exc)
            results.append(create_result(link, False, error=str(exc)))
            continue

        logger.info("Starting bot %d/%d: %s", index, total, bot_username)

        try:
            download_path = await run_bot_flow(config.with_bot(bot_username))
            if not download_path:
                error = "No downloadable price list was produced"
                logger.warning("Bot %s failed: %s", bot_username, error)
                results.append(create_result(link, False, error=error))
            else:
                logger.info(
                    "Bot %s succeeded with download %s",
                    bot_username,
                    format_path(download_path),
                )
                results.append(create_result(link, True, download_path=download_path))
        except Exception as exc:
            logger.exception("Bot %s failed with an unexpected error", bot_username)
            results.append(create_result(link, False, error=str(exc)))

        if index < total:
            await randomized_batch_delay()

    return results


def write_run_reports(
    results: list[BatchDownloadResult],
    *,
    reports_dir: Path,
    source_label: str,
    source_path: Path | None,
) -> tuple[Path, Path, Path]:
    reports_dir.mkdir(parents=True, exist_ok=True)
    run_timestamp = datetime.now(timezone.utc)
    timestamp_label = run_timestamp.strftime("%Y%m%dT%H%M%SZ")

    successes = [result for result in results if result.success]
    failures = [result for result in results if not result.success]

    summary = {
        "timestamp": run_timestamp.isoformat(),
        "source_label": source_label,
        "source_path": format_path(source_path) if source_path else None,
        "total": len(results),
        "success_count": len(successes),
        "failure_count": len(failures),
        "results": [asdict(result) for result in results],
    }

    summary_path = reports_dir / f"download_summary_{timestamp_label}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    latest_summary_path = reports_dir / "latest_download_summary.json"
    latest_summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    failed_links_path = reports_dir / f"failed_links_{timestamp_label}.txt"
    failed_links_path.write_text(
        "\n".join(result.link for result in failures) + ("\n" if failures else ""),
        encoding="utf-8",
    )

    latest_failed_links_path = get_retry_failed_path(reports_dir)
    latest_failed_links_path.write_text(
        "\n".join(result.link for result in failures) + ("\n" if failures else ""),
        encoding="utf-8",
    )

    return summary_path, latest_summary_path, failed_links_path


def log_run_summary(
    results: list[BatchDownloadResult],
    *,
    summary_path: Path,
    failed_links_path: Path,
) -> None:
    successes = [result for result in results if result.success]
    failures = [result for result in results if not result.success]

    logger.info(
        "Batch complete: %d succeeded, %d failed, %d total",
        len(successes),
        len(failures),
        len(results),
    )
    logger.info("Wrote batch summary to %s", format_path(summary_path))

    if failures:
        logger.warning(
            "Failed bots: %s",
            summarize_items([result.bot_username for result in failures], limit=10),
        )
        logger.warning("Failed link list written to %s", format_path(failed_links_path))


async def main() -> None:
    args = build_argument_parser().parse_args()

    fetch_dir = Path(os.environ.get("TG_FETCH_DIR", "outputs/fetched_links"))
    reports_dir = Path(os.environ.get("TG_DOWNLOAD_REPORT_DIR", "outputs/download_runs"))
    source_label, source_path, links = load_links_for_run(fetch_dir, reports_dir, args.retry_failed)

    if not links:
        logger.warning("No Telegram links found for %s", source_label)
        return

    logger.info(
        "Using %s source %s with %d link(s)",
        source_label,
        format_path(source_path) if source_path else "none",
        len(links),
    )

    config = BotFlowConfig.from_env()
    results = await process_links(links, config)

    summary_path, _, failed_links_path = write_run_reports(
        results,
        reports_dir=reports_dir,
        source_label=source_label,
        source_path=source_path,
    )
    log_run_summary(results, summary_path=summary_path, failed_links_path=failed_links_path)


if __name__ == "__main__":
    asyncio.run(main())
