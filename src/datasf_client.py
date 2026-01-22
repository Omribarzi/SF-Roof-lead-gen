"""San Francisco Open Data API client."""
import httpx
from typing import Any

from .config import (
    DATASF_PERMITS_URL,
    DATASF_TAX_ROLLS_URL,
    DATASF_APP_TOKEN,
    DEFAULT_PAGE_SIZE,
)
from .utils import format_block_lot, get_cutoff_date


class DataSFClient:
    """Client for interacting with SF Open Data (SODA) API."""

    def __init__(self, app_token: str | None = None):
        self.app_token = app_token or DATASF_APP_TOKEN
        self.headers = {}
        if self.app_token:
            self.headers["X-App-Token"] = self.app_token

    async def _fetch_paginated(
        self,
        url: str,
        params: dict[str, str],
        limit: int | None = None
    ) -> list[dict[str, Any]]:
        """Fetch all records with pagination support."""
        all_records = []
        offset = 0
        page_size = DEFAULT_PAGE_SIZE

        async with httpx.AsyncClient(timeout=60.0) as client:
            while True:
                page_params = {
                    **params,
                    "$limit": str(page_size),
                    "$offset": str(offset),
                }

                response = await client.get(url, params=page_params, headers=self.headers)
                response.raise_for_status()
                records = response.json()

                if not records:
                    break

                all_records.extend(records)

                # Check if we've hit the user-specified limit
                if limit and len(all_records) >= limit:
                    all_records = all_records[:limit]
                    break

                # Check if we got fewer records than page size (last page)
                if len(records) < page_size:
                    break

                offset += page_size
                print(f"  Fetched {len(all_records)} records...")

        return all_records

    async def get_residential_properties(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Fetch single-family and 2-family dwellings from tax rolls.

        Returns properties with block_lot identifier and relevant metadata.
        """
        print("Fetching residential properties from Tax Rolls...")

        params = {
            "$where": "use_definition like '%family dwelling%'",
            "$select": "block,lot,property_location,use_definition,year_property_built",
        }

        records = await self._fetch_paginated(DATASF_TAX_ROLLS_URL, params, limit)

        # Process and add block_lot identifier
        properties = []
        for record in records:
            block_lot = format_block_lot(record.get("block"), record.get("lot"))
            if block_lot:
                properties.append({
                    "block": record.get("block"),
                    "lot": record.get("lot"),
                    "block_lot": block_lot,
                    "address": record.get("property_location", ""),
                    "property_type": record.get("use_definition", ""),
                    "year_built": record.get("year_property_built"),
                })

        return properties

    async def get_roofing_permits(self, years: int = 15, limit: int | None = None) -> list[dict[str, Any]]:
        """Fetch all roofing permits from the last N years.

        Returns permits with block_lot identifier for matching.
        """
        print(f"Fetching roofing permits from last {years} years...")

        cutoff_date = get_cutoff_date(years)

        # Query for roofing-related permits
        params = {
            "$where": f"(description like '%ROOF%' OR description like '%ROOFING%') AND filed_date > '{cutoff_date}'",
            "$select": "block,lot,street_number,street_name,street_suffix,filed_date,description",
        }

        records = await self._fetch_paginated(DATASF_PERMITS_URL, params, limit)

        # Process and add block_lot identifier
        permits = []
        for record in records:
            block_lot = format_block_lot(record.get("block"), record.get("lot"))
            if block_lot:
                permits.append({
                    "block": record.get("block"),
                    "lot": record.get("lot"),
                    "block_lot": block_lot,
                    "filed_date": record.get("filed_date"),
                    "description": record.get("description", ""),
                })

        return permits
