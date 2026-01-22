#!/usr/bin/env python3
"""Entry point for SF Roofing Leads Generator."""
import argparse
import asyncio
import sys

from src.pipeline import run_pipeline
from src.config import DEFAULT_YEARS_LOOKBACK


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate roofing leads from SF property data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test with 10 records (no skip tracing)
  python main.py --limit 10 --skip-enrichment

  # Full run without skip tracing (free)
  python main.py --skip-enrichment

  # Full run with skip tracing (costs money)
  python main.py

  # Custom lookback period
  python main.py --years 20 --skip-enrichment
        """
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of properties to process (for testing)"
    )

    parser.add_argument(
        "--skip-enrichment",
        action="store_true",
        help="Skip skip-tracing enrichment step"
    )

    parser.add_argument(
        "--years",
        type=int,
        default=DEFAULT_YEARS_LOOKBACK,
        help=f"Years to look back for roofing permits (default: {DEFAULT_YEARS_LOOKBACK})"
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        default="output",
        help="Output directory for CSV files (default: output)"
    )

    return parser.parse_args()


async def main() -> int:
    """Main entry point."""
    args = parse_args()

    print("=" * 60)
    print("SF Roofing Leads Generator")
    print("=" * 60)
    print(f"Settings:")
    print(f"  - Limit: {args.limit or 'None (all records)'}")
    print(f"  - Skip enrichment: {args.skip_enrichment}")
    print(f"  - Years lookback: {args.years}")
    print(f"  - Output directory: {args.output_dir}")
    print("=" * 60)

    try:
        leads = await run_pipeline(
            limit=args.limit,
            skip_enrichment=args.skip_enrichment,
            years_lookback=args.years,
            output_dir=args.output_dir
        )

        print("=" * 60)
        print(f"Pipeline complete! Generated {len(leads)} leads.")
        print("=" * 60)

        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
