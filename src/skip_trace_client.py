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

        DataSF format examples:
        - "0000 0710 NORTH POINT         ST0000"
        - "2710 2706 HYDE                ST0000"

        We need: street, city, state (clean format like "710 NORTH POINT ST")
        """
        import re

        # Normalize whitespace
        addr = " ".join(address.split())

        # Remove trailing "0000" or similar codes
        addr = re.sub(r'\d{4}$', '', addr).strip()

        # Split "ST" suffix that's attached to name (e.g., "POINTST" -> "POINT ST")
        addr = re.sub(r'([A-Z]{2,})(ST|AVE|BLVD|DR|CT|PL|WAY|RD|LN|TER|CIR)$', r'\1 \2', addr)

        # Parse: find first non-zero number as street number
        words = addr.split()
        street_num = None
        street_name_parts = []

        for word in words:
            if street_num is None and word.isdigit():
                num = int(word)
                if num > 0:
                    street_num = str(num)  # Remove leading zeros
            elif street_num is not None:
                street_name_parts.append(word)
            # Skip leading zeros

        if street_num and street_name_parts:
            street = f"{street_num} {' '.join(street_name_parts)}"
        else:
            street = addr  # Fallback

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

        # Phones: filter by quality
        # Priority: tested+reachable, recent data, high score, not DNC
        for phone in person.get("phoneNumbers", []):
            phone_num = phone.get("number", "")
            if not phone_num or phone_num in [p[0] for p in phones]:
                continue

            # Quality checks
            tested = phone.get("tested", False)
            reachable = phone.get("reachable", False)
            score = phone.get("score", 0)
            dnc = phone.get("dnc", False)
            last_reported = phone.get("lastReportedDate", "")
            phone_type = phone.get("type", "")

            # Skip DNC numbers (illegal to cold call)
            if dnc:
                continue

            # Calculate quality score
            quality = 0
            is_recent = last_reported and last_reported >= "2024"
            is_verified = tested and reachable

            if is_verified:
                quality += 50
            if is_recent:
                quality += 40
            if score and int(score) >= 95:
                quality += 20
            elif score and int(score) >= 90:
                quality += 10
            if phone_type == "Mobile":
                quality += 10

            # STRICT: Only include if verified OR recent data
            if is_verified or is_recent or (score and int(score) == 100 and reachable):
                phones.append((phone_num, quality, phone_type, is_verified, is_recent))

        # Sort by quality, take best numbers
        phones.sort(key=lambda x: x[1], reverse=True)

        # Track verification status
        has_verified_phone = any(p[3] for p in phones)  # tested+reachable
        has_recent_phone = any(p[4] for p in phones)    # 2024+ data

        # Extract just the numbers
        phone_numbers = [p[0] for p in phones]

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

        # Determine data quality level
        if has_verified_phone:
            data_quality = "verified"
        elif has_recent_phone:
            data_quality = "recent"
        elif phone_numbers:
            data_quality = "unverified"
        else:
            data_quality = "no_phone"

        return {
            "owner_name": owner_name,
            "phone_1": phone_numbers[0] if len(phone_numbers) > 0 else "",
            "phone_2": phone_numbers[1] if len(phone_numbers) > 1 else "",
            "phone_3": phone_numbers[2] if len(phone_numbers) > 2 else "",
            "email_1": emails[0] if len(emails) > 0 else "",
            "email_2": emails[1] if len(emails) > 1 else "",
            "mailing_address": mailing_address,
            "data_quality": data_quality,
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

                # Build request for batch using propertyAddress object
                requests_data = []
                for prop in batch:
                    addr_parts = self._parse_address(prop.get("address", ""))
                    requests_data.append({
                        "propertyAddress": {
                            "street": addr_parts["street"],
                            "city": addr_parts["city"],
                            "state": addr_parts["state"],
                        }
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
            "data_quality": "no_data",
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
