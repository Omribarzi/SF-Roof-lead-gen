"""BatchData Skip Tracing API client for owner enrichment."""
import httpx
from typing import Any

from .config import SKIP_TRACE_API_KEY, SKIP_TRACE_API_URL, DEFAULT_BATCH_SIZE
from .utils import chunk_list


class BatchDataClient:
    """Client for BatchData.io skip tracing API."""

    # BatchData API endpoint
    BASE_URL = "https://api.batchdata.com/api/v1"

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or SKIP_TRACE_API_KEY
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _parse_address(self, address: str) -> dict[str, str]:
        """Parse SF DataSF address format into components.

        DataSF format: "0000 0710 NORTH POINT         ST0000" (messy)
        We need: street, city, state
        """
        # Clean up the address - remove extra spaces and trailing codes
        addr = " ".join(address.split())  # normalize whitespace

        # Remove trailing zip-like codes (0000)
        parts = addr.rsplit(" ", 1)
        if parts and parts[-1].isdigit():
            addr = parts[0]

        # Remove leading zeros/numbers that aren't part of street number
        words = addr.split()
        # Find first non-zero street number
        street_parts = []
        found_street_num = False
        for word in words:
            if not found_street_num and word.isdigit():
                if int(word) > 0:
                    street_parts.append(word)
                    found_street_num = True
            else:
                street_parts.append(word)

        street = " ".join(street_parts) if street_parts else addr

        return {
            "street": street,
            "city": "San Francisco",
            "state": "CA",
        }

    def _extract_contacts(self, person: dict) -> dict[str, Any]:
        """Extract owner info from BatchData person object."""
        owner_name = ""
        phones = []
        emails = []
        mailing_address = ""

        # Name: {first, last}
        name = person.get("name", {})
        name_parts = []
        if name.get("first"):
            name_parts.append(name["first"].title())
        if name.get("last"):
            name_parts.append(name["last"].title())
        owner_name = " ".join(name_parts)

        # Phones: [{number, type, ...}]
        for phone in person.get("phoneNumbers", []):
            phone_num = phone.get("number", "")
            if phone_num and phone_num not in phones:
                phones.append(phone_num)

        # Emails: [{email}]
        for email in person.get("emails", []):
            email_addr = email.get("email", "")
            if email_addr and email_addr not in emails:
                emails.append(email_addr)

        # Mailing address from property.owner.mailingAddress
        prop = person.get("property", {})
        owner_info = prop.get("owner", {})
        mailing = owner_info.get("mailingAddress", {})
        if mailing:
            mailing_parts = [
                mailing.get("street", ""),
                mailing.get("city", ""),
                mailing.get("state", ""),
                mailing.get("zip", ""),
            ]
            mailing_address = ", ".join(p for p in mailing_parts if p)

        return {
            "owner_name": owner_name,
            "phone_1": phones[0] if len(phones) > 0 else "",
            "phone_2": phones[1] if len(phones) > 1 else "",
            "phone_3": phones[2] if len(phones) > 2 else "",
            "email_1": emails[0] if len(emails) > 0 else "",
            "email_2": emails[1] if len(emails) > 1 else "",
            "mailing_address": mailing_address,
        }

    async def skip_trace_batch(
        self,
        properties: list[dict[str, Any]],
        batch_size: int = 100  # BatchData supports up to 100 per request
    ) -> list[dict[str, Any]]:
        """Skip trace a batch of properties.

        Uses BatchData's /property/skip-trace endpoint.
        """
        if not self.api_key:
            print("Warning: No BatchData API key configured. Skipping enrichment.")
            return self._empty_enrichment(properties)

        print(f"Skip tracing {len(properties)} properties via BatchData...")

        enriched = []
        batches = chunk_list(properties, batch_size)

        async with httpx.AsyncClient(timeout=60.0) as client:
            for i, batch in enumerate(batches):
                print(f"  Processing batch {i + 1}/{len(batches)} ({len(batch)} properties)...")

                # Build request for batch
                requests_data = []
                for prop in batch:
                    addr_parts = self._parse_address(prop.get("address", ""))
                    requests_data.append({
                        "street": addr_parts["street"],
                        "city": addr_parts["city"],
                        "state": addr_parts["state"],
                    })

                try:
                    response = await client.post(
                        f"{self.BASE_URL}/property/skip-trace",
                        headers=self.headers,
                        json={"requests": requests_data}
                    )

                    if response.status_code == 200:
                        data = response.json()
                        # BatchData returns {results: {persons: [...]}}
                        persons = data.get("results", {}).get("persons", [])

                        for j, prop in enumerate(batch):
                            if j < len(persons) and persons[j].get("meta", {}).get("matched"):
                                contacts = self._extract_contacts(persons[j])
                                enriched.append({
                                    **prop,
                                    **contacts,
                                    "enrichment_status": "success",
                                })
                            else:
                                enriched.append({
                                    **prop,
                                    **self._empty_contacts(),
                                    "enrichment_status": "no_match",
                                })
                    else:
                        print(f"    Error: {response.status_code} - {response.text[:200]}")
                        for prop in batch:
                            enriched.append({
                                **prop,
                                **self._empty_contacts(),
                                "enrichment_status": f"error_{response.status_code}",
                            })

                except Exception as e:
                    print(f"    Exception: {e}")
                    for prop in batch:
                        enriched.append({
                            **prop,
                            **self._empty_contacts(),
                            "enrichment_status": f"error_{str(e)[:50]}",
                        })

        success_count = sum(1 for e in enriched if e.get("enrichment_status") == "success")
        print(f"  Enriched {success_count}/{len(properties)} properties successfully")

        return enriched

    def _empty_contacts(self) -> dict[str, str]:
        """Return empty contact fields."""
        return {
            "owner_name": "",
            "phone_1": "",
            "phone_2": "",
            "phone_3": "",
            "email_1": "",
            "email_2": "",
            "mailing_address": "",
        }

    def _empty_enrichment(self, properties: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Return properties with empty enrichment."""
        return [
            {**p, **self._empty_contacts(), "enrichment_status": "skipped"}
            for p in properties
        ]


# Alias for backward compatibility
SkipTraceClient = BatchDataClient


class MockSkipTraceClient(BatchDataClient):
    """Mock client for testing without hitting the real API."""

    async def skip_trace_batch(
        self,
        properties: list[dict[str, Any]],
        batch_size: int = 100
    ) -> list[dict[str, Any]]:
        """Return properties with empty enrichment fields."""
        print(f"Mock enrichment: {len(properties)} properties (no API calls)")
        return [
            {**p, **self._empty_contacts(), "enrichment_status": "mock"}
            for p in properties
        ]

    # Alias for compatibility
    async def enrich_batch(self, properties: list[dict[str, Any]], batch_size: int = 100):
        return await self.skip_trace_batch(properties, batch_size)
