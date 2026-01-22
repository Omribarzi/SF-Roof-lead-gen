"""Main pipeline orchestration for lead generation."""
import pandas as pd
from datetime import date
from pathlib import Path
from typing import Any

from .datasf_client import DataSFClient
from .skip_trace_client import SkipTraceClient, MockSkipTraceClient
from .utils import deduplicate_by_key, calculate_days_since
from .config import DEFAULT_YEARS_LOOKBACK


def export_to_csv(records: list[dict[str, Any]], output_path: str) -> None:
    """Export enriched records to CSV."""
    if not records:
        print("No records to export.")
        return

    df = pd.DataFrame(records)

    # Reorder columns for output
    column_order = [
        "address",
        "block_lot",
        "owner_name",
        "phone_1",
        "phone_2",
        "phone_3",
        "email_1",
        "email_2",
        "mailing_address",
        "data_quality",
        "property_type",
        "year_built",
        "last_roof_permit_date",
        "days_since_last_permit",
    ]

    # Only include columns that exist
    existing_columns = [col for col in column_order if col in df.columns]
    df = df[existing_columns]

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_path, index=False)
    print(f"Exported {len(records)} leads to {output_path}")


async def run_pipeline(
    limit: int | None = None,
    skip_enrichment: bool = False,
    years_lookback: int = DEFAULT_YEARS_LOOKBACK,
    output_dir: str = "output"
) -> list[dict[str, Any]]:
    """Run the full lead generation pipeline.

    Args:
        limit: Maximum number of properties to process (for testing)
        skip_enrichment: If True, skip the skip tracing step
        years_lookback: Number of years to check for roofing permits
        output_dir: Directory for output CSV files

    Returns:
        List of enriched lead records
    """
    datasf = DataSFClient()

    # 1. Fetch all single-family properties
    properties = await datasf.get_residential_properties(limit=limit)
    print(f"Found {len(properties)} residential properties")

    if not properties:
        print("No properties found. Exiting.")
        return []

    # Deduplicate by block_lot
    properties = deduplicate_by_key(properties, "block_lot")
    print(f"After deduplication: {len(properties)} unique properties")

    # 2. Fetch all roofing permits (last N years)
    roof_permits = await datasf.get_roofing_permits(years=years_lookback)
    print(f"Found {len(roof_permits)} recent roofing permits")

    # 3. Create set of block_lots with recent permits
    permitted_block_lots = {p["block_lot"] for p in roof_permits}

    # Create a mapping of block_lot to most recent permit date
    permit_dates: dict[str, str] = {}
    for permit in roof_permits:
        block_lot = permit["block_lot"]
        filed_date = permit.get("filed_date", "")
        if block_lot not in permit_dates or filed_date > permit_dates[block_lot]:
            permit_dates[block_lot] = filed_date

    # 4. Filter to properties WITHOUT recent permits
    leads = [
        p for p in properties
        if p["block_lot"] not in permitted_block_lots
    ]
    print(f"Found {len(leads)} properties without recent roof permits")

    if not leads:
        print("No leads found after filtering. Exiting.")
        return []

    # 5. Add permit date info (all will be None/empty since they have no recent permits)
    for lead in leads:
        lead["last_roof_permit_date"] = None
        lead["days_since_last_permit"] = 9999  # No recent permit

    # 6. Skip trace enrichment via BatchData
    if skip_enrichment:
        skip_tracer = MockSkipTraceClient()
    else:
        skip_tracer = SkipTraceClient()

    enriched = await skip_tracer.skip_trace_batch(leads)

    # 7. Export to CSV
    output_path = f"{output_dir}/leads_{date.today()}.csv"
    export_to_csv(enriched, output_path)

    return enriched
