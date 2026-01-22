"""Utility functions for data processing."""
from datetime import datetime, timedelta
from typing import Any


def format_block_lot(block: str | None, lot: str | None) -> str:
    """Format block and lot into a unique identifier.

    Block is typically 4 digits, lot is 3 digits (padded with zeros).
    """
    if not block or not lot:
        return ""
    block_str = str(block).zfill(4)
    lot_str = str(lot).zfill(3)
    return f"{block_str}-{lot_str}"


def parse_block_lot(block_lot: str) -> tuple[str, str]:
    """Parse a block-lot string into components."""
    parts = block_lot.split("-")
    if len(parts) != 2:
        return "", ""
    return parts[0], parts[1]


def format_address(
    street_number: str | None,
    street_name: str | None,
    street_suffix: str | None = None,
    city: str = "San Francisco",
    state: str = "CA"
) -> str:
    """Format address components into a full address string."""
    parts = []
    if street_number:
        parts.append(str(street_number))
    if street_name:
        parts.append(str(street_name))
    if street_suffix:
        parts.append(str(street_suffix))

    street = " ".join(parts)
    if street:
        return f"{street}, {city}, {state}"
    return ""


def get_cutoff_date(years: int = 15) -> str:
    """Get the cutoff date for permit filtering (ISO format)."""
    cutoff = datetime.now() - timedelta(days=years * 365)
    return cutoff.strftime("%Y-%m-%d")


def calculate_days_since(date_str: str | None) -> int:
    """Calculate days since a given date string."""
    if not date_str:
        return 9999  # No permit on record

    try:
        # Handle various date formats
        for fmt in ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d", "%m/%d/%Y"]:
            try:
                date = datetime.strptime(date_str[:19], fmt[:len(date_str)])
                break
            except ValueError:
                continue
        else:
            return 9999

        delta = datetime.now() - date
        return delta.days
    except Exception:
        return 9999


def deduplicate_by_key(records: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    """Deduplicate a list of records by a specific key."""
    seen = set()
    result = []
    for record in records:
        key_value = record.get(key)
        if key_value and key_value not in seen:
            seen.add(key_value)
            result.append(record)
    return result


def chunk_list(lst: list, chunk_size: int) -> list[list]:
    """Split a list into chunks of specified size."""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
