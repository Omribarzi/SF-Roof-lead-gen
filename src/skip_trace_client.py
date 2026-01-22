"""Skip Tracing API client for owner enrichment."""
import httpx
from typing import Any

from .config import SKIP_TRACE_API_KEY, SKIP_TRACE_API_URL, DEFAULT_BATCH_SIZE
from .utils import chunk_list


class SkipTraceClient:
    """Client for skip tracing API to enrich property data with owner info."""

    def __init__(self, api_key: str | None = None, api_url: str | None = None):
        self.api_key = api_key or SKIP_TRACE_API_KEY
        self.api_url = api_url or SKIP_TRACE_API_URL
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _parse_address(self, address: str) -> dict[str, str]:
        """Parse address string into components for API request."""
        # Simple parsing - address format: "123 Main St, San Francisco, CA"
        parts = address.split(",")
        street = parts[0].strip() if parts else ""
        city = parts[1].strip() if len(parts) > 1 else "San Francisco"
        state = parts[2].strip() if len(parts) > 2 else "CA"

        return {
            "street": street,
            "city": city,
            "state": state,
        }

    async def _enrich_single(
        self,
        client: httpx.AsyncClient,
        property_data: dict[str, Any]
    ) -> dict[str, Any]:
        """Enrich a single property with owner information."""
        address = property_data.get("address", "")
        if not address:
            return {**property_data, "enrichment_status": "no_address"}

        address_parts = self._parse_address(address)

        try:
            response = await client.post(
                f"{self.api_url}/search",
                headers=self.headers,
                json={
                    "address": address_parts["street"],
                    "city": address_parts["city"],
                    "state": address_parts["state"],
                }
            )

            if response.status_code == 200:
                data = response.json()
                return {
                    **property_data,
                    "owner_name": data.get("owner_name", ""),
                    "phone_1": data.get("phone_numbers", [""])[0] if data.get("phone_numbers") else "",
                    "phone_2": data.get("phone_numbers", ["", ""])[1] if len(data.get("phone_numbers", [])) > 1 else "",
                    "phone_3": data.get("phone_numbers", ["", "", ""])[2] if len(data.get("phone_numbers", [])) > 2 else "",
                    "email_1": data.get("email_addresses", [""])[0] if data.get("email_addresses") else "",
                    "email_2": data.get("email_addresses", ["", ""])[1] if len(data.get("email_addresses", [])) > 1 else "",
                    "mailing_address": data.get("mailing_address", ""),
                    "enrichment_status": "success",
                }
            else:
                return {**property_data, "enrichment_status": f"error_{response.status_code}"}

        except Exception as e:
            return {**property_data, "enrichment_status": f"error_{str(e)}"}

    async def enrich_batch(
        self,
        properties: list[dict[str, Any]],
        batch_size: int = DEFAULT_BATCH_SIZE
    ) -> list[dict[str, Any]]:
        """Enrich a batch of properties with owner information.

        Processes in batches to avoid timeouts and rate limits.
        """
        if not self.api_key:
            print("Warning: No skip trace API key configured. Skipping enrichment.")
            return [
                {
                    **p,
                    "owner_name": "",
                    "phone_1": "",
                    "phone_2": "",
                    "phone_3": "",
                    "email_1": "",
                    "email_2": "",
                    "mailing_address": "",
                    "enrichment_status": "skipped",
                }
                for p in properties
            ]

        print(f"Enriching {len(properties)} properties with skip tracing...")

        enriched = []
        batches = chunk_list(properties, batch_size)

        async with httpx.AsyncClient(timeout=30.0) as client:
            for i, batch in enumerate(batches):
                print(f"  Processing batch {i + 1}/{len(batches)}...")

                for prop in batch:
                    result = await self._enrich_single(client, prop)
                    enriched.append(result)

        return enriched


class MockSkipTraceClient(SkipTraceClient):
    """Mock client for testing without hitting the real API."""

    async def enrich_batch(
        self,
        properties: list[dict[str, Any]],
        batch_size: int = DEFAULT_BATCH_SIZE
    ) -> list[dict[str, Any]]:
        """Return properties with empty enrichment fields."""
        print(f"Mock enrichment: {len(properties)} properties (no API calls)")
        return [
            {
                **p,
                "owner_name": "",
                "phone_1": "",
                "phone_2": "",
                "phone_3": "",
                "email_1": "",
                "email_2": "",
                "mailing_address": "",
                "enrichment_status": "mock",
            }
            for p in properties
        ]
