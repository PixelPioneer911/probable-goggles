import asyncio
import os
import random
from dataclasses import dataclass
from pathlib import Path

from telethon import TelegramClient

from logging_utils import configure_logging, format_path, summarize_items

logger = configure_logging(__name__)


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


@dataclass(frozen=True)
class BotFlowConfig:
    api_id: int
    api_hash: str
    session_name: str
    bot_username: str
    message_text: str
    wait_timeout_seconds: int
    download_dir: Path
    min_action_delay_seconds: float
    max_action_delay_seconds: float
    min_poll_delay_seconds: float
    max_poll_delay_seconds: float
    max_steps: int = 9

    @classmethod
    def from_env(cls) -> "BotFlowConfig":
        load_dotenv(Path(".env"))
        return cls(
            api_id=int(os.environ["TG_API_ID"]),
            api_hash=os.environ["TG_API_HASH"],
            session_name=os.environ.get("TG_SESSION_NAME", "telegram_test"),
            bot_username=os.environ["TG_TEST_BOT"],
            message_text=os.environ.get("TG_MESSAGE", "/start"),
            wait_timeout_seconds=int(os.environ.get("TG_WAIT_TIMEOUT", "15")),
            download_dir=Path(os.environ.get("TG_DOWNLOAD_DIR", "outputs/downloads")),
            min_action_delay_seconds=float(os.environ.get("TG_MIN_ACTION_DELAY", "0.8")),
            max_action_delay_seconds=float(os.environ.get("TG_MAX_ACTION_DELAY", "2.0")),
            min_poll_delay_seconds=float(os.environ.get("TG_MIN_POLL_DELAY", "1.2")),
            max_poll_delay_seconds=float(os.environ.get("TG_MAX_POLL_DELAY", "2.4")),
        )

    def with_bot(self, bot_username: str) -> "BotFlowConfig":
        return BotFlowConfig(
            api_id=self.api_id,
            api_hash=self.api_hash,
            session_name=self.session_name,
            bot_username=bot_username,
            message_text=self.message_text,
            wait_timeout_seconds=self.wait_timeout_seconds,
            download_dir=self.download_dir,
            min_action_delay_seconds=self.min_action_delay_seconds,
            max_action_delay_seconds=self.max_action_delay_seconds,
            min_poll_delay_seconds=self.min_poll_delay_seconds,
            max_poll_delay_seconds=self.max_poll_delay_seconds,
            max_steps=self.max_steps,
        )


def get_button_labels(message) -> list[str]:
    labels = []
    for row in getattr(message, "buttons", None) or []:
        for button in row:
            text = getattr(button, "text", None)
            if text:
                labels.append(text)
    return labels


def normalize_label(text: str) -> str:
    return " ".join(text.casefold().split())


def labels_signature(labels: list[str]) -> tuple[str, ...]:
    return tuple(normalize_label(label) for label in labels)


def find_matching_label(labels: list[str], *wanted_terms: str) -> str | None:
    normalized_labels = [(label, normalize_label(label)) for label in labels]
    for wanted in wanted_terms:
        normalized_wanted = normalize_label(wanted)
        for label, normalized_label in normalized_labels:
            if normalized_wanted in normalized_label:
                return label
    return None


async def randomized_delay(config: BotFlowConfig) -> None:
    lower = min(config.min_action_delay_seconds, config.max_action_delay_seconds)
    upper = max(config.min_action_delay_seconds, config.max_action_delay_seconds)
    delay_seconds = random.uniform(lower, upper)
    logger.debug("Waiting %.2fs before next action", delay_seconds)
    await asyncio.sleep(delay_seconds)


async def randomized_poll_delay(config: BotFlowConfig) -> None:
    lower = min(config.min_poll_delay_seconds, config.max_poll_delay_seconds)
    upper = max(config.min_poll_delay_seconds, config.max_poll_delay_seconds)
    delay_seconds = random.uniform(lower, upper)
    logger.debug("Waiting %.2fs before checking for the next bot update", delay_seconds)
    await asyncio.sleep(delay_seconds)


async def click_button_by_label(message, chosen_label: str) -> None:
    wanted = normalize_label(chosen_label)
    result = await message.click(text=lambda text: normalize_label(text) == wanted)
    logger.info(
        "Clicked button %r on message %s",
        chosen_label,
        getattr(message, "id", "unknown"),
    )
    logger.debug("Button click result: %r", result)


async def wait_for_keyboard_message(
    client: TelegramClient,
    config: BotFlowConfig,
    chat: str,
    timeout_seconds: int,
    *,
    min_message_id: int = 0,
    active_message_id: int | None = None,
    previous_labels: list[str] | None = None,
):
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    previous_sig = labels_signature(previous_labels or [])

    while asyncio.get_running_loop().time() < deadline:
        if active_message_id is not None:
            current = await client.get_messages(chat, ids=active_message_id)
            if current:
                current_labels = get_button_labels(current)
                if current_labels and labels_signature(current_labels) != previous_sig:
                    return current, current_labels

        messages = await client.get_messages(chat, limit=10, min_id=min_message_id)
        for message in sorted(messages, key=lambda item: item.id, reverse=True):
            if getattr(message, "out", False):
                continue
            labels = get_button_labels(message)
            if labels:
                return message, labels

        await randomized_poll_delay(config)

    return None, []


async def wait_for_latest_incoming_message(
    client: TelegramClient,
    config: BotFlowConfig,
    chat: str,
    timeout_seconds: int,
    *,
    min_message_id: int = 0,
):
    deadline = asyncio.get_running_loop().time() + timeout_seconds

    while asyncio.get_running_loop().time() < deadline:
        messages = await client.get_messages(chat, limit=10, min_id=min_message_id)
        for message in sorted(messages, key=lambda item: item.id, reverse=True):
            if getattr(message, "out", False):
                continue
            return message
        await randomized_poll_delay(config)

    return None


async def download_media_from_message(
    client: TelegramClient,
    config: BotFlowConfig,
    chat: str,
    timeout_seconds: int,
    download_dir: Path,
    *,
    min_message_id: int,
    active_message_id: int | None = None,
) -> Path | None:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    message = None

    while asyncio.get_running_loop().time() < deadline:
        if active_message_id is not None:
            current = await client.get_messages(chat, ids=active_message_id)
            if current and getattr(current, "media", None):
                message = current
                break

        latest_message = await wait_for_latest_incoming_message(
            client,
            config,
            chat,
            1,
            min_message_id=min_message_id,
        )
        if latest_message and getattr(latest_message, "media", None):
            message = latest_message
            break

        await randomized_poll_delay(config)

    if not message:
        logger.warning(
            "No media message arrived within %ss after clicking the price list button",
            timeout_seconds,
        )
        return None

    download_dir.mkdir(parents=True, exist_ok=True)
    downloaded = await client.download_media(message, file=download_dir)
    if not downloaded:
        logger.error("Failed to download media from message %s", message.id)
        return None

    downloaded_path = Path(downloaded)
    logger.info(
        "Downloaded media from message %s to %s",
        message.id,
        format_path(downloaded_path),
    )
    return downloaded_path


def choose_button(labels: list[str], reached_main_menu: bool) -> tuple[str | None, bool]:
    listings_label = find_matching_label(labels, "listings")
    price_list_label = find_matching_label(labels, "price list", "pricelist")

    if price_list_label:
        return price_list_label, True
    if listings_label:
        return listings_label, False
    if reached_main_menu:
        return None, False

    return (
        find_matching_label(
            labels,
            "yes",
            "i understand and agree",
            "i understand",
            "agree",
            "understand",
        ),
        False,
    )


async def inspect_keyboard_flow(client: TelegramClient, config: BotFlowConfig) -> Path | None:
    highest_seen_message_id = 0
    active_message_id: int | None = None
    last_labels: list[str] = []
    reached_main_menu = False

    for step in range(1, config.max_steps + 1):
        message, labels = await wait_for_keyboard_message(
            client,
            config,
            config.bot_username,
            config.wait_timeout_seconds,
            min_message_id=highest_seen_message_id,
            active_message_id=active_message_id,
            previous_labels=last_labels,
        )

        if not message:
            logger.warning(
                "No keyboard message arrived within %ss on step %s",
                config.wait_timeout_seconds,
                step,
            )
            return None

        highest_seen_message_id = max(highest_seen_message_id, message.id)
        active_message_id = message.id
        last_labels = labels

        logger.info(
            "Step %s received keyboard on message %s with %d button(s): %s",
            step,
            message.id,
            len(labels),
            summarize_items(labels, limit=6),
        )

        chosen_label, is_price_list = choose_button(labels, reached_main_menu)
        if find_matching_label(labels, "listings"):
            reached_main_menu = True

        if not chosen_label:
            logger.warning(
                "No matching button found on step %s. Available buttons: %s",
                step,
                summarize_items(labels, limit=8),
            )
            return None

        try:
            await click_button_by_label(message, chosen_label)
        except Exception:
            logger.exception("Failed to click %r on step %s", chosen_label, step)
            return None

        await randomized_delay(config)

        if is_price_list:
            return await download_media_from_message(
                client,
                config,
                config.bot_username,
                config.wait_timeout_seconds,
                config.download_dir,
                min_message_id=highest_seen_message_id,
                active_message_id=active_message_id,
            )

    logger.warning(
        "Stopped after %s steps without reaching a downloadable price list response",
        config.max_steps,
    )
    return None


async def run_bot_flow(config: BotFlowConfig) -> Path | None:
    async with TelegramClient(config.session_name, config.api_id, config.api_hash) as client:
        sent = await client.send_message(config.bot_username, config.message_text)
        logger.info(
            "Sent %r to %s as message %s",
            config.message_text,
            config.bot_username,
            sent.id,
        )
        logger.info(
            "Configured delays: action %.1f-%.1fs, poll %.1f-%.1fs",
            min(config.min_action_delay_seconds, config.max_action_delay_seconds),
            max(config.min_action_delay_seconds, config.max_action_delay_seconds),
            min(config.min_poll_delay_seconds, config.max_poll_delay_seconds),
            max(config.min_poll_delay_seconds, config.max_poll_delay_seconds),
        )
        await randomized_delay(config)
        return await inspect_keyboard_flow(client, config)


async def main() -> None:
    await run_bot_flow(BotFlowConfig.from_env())


if __name__ == "__main__":
    asyncio.run(main())
