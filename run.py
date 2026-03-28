"""
run.py — Unified CLI entry point for the Groovon pipeline.

Supports pluggable sources and sinks:
  python run.py excel --input data.xlsx --output result.xlsx
  python run.py scraper --json _archive/scraper/output/events_london_latest.json
  python run.py api --city London --days 30 --genre "Jazz,Blues" --venue "Ronnie Scott" --artist "Ezra"

Backward-compatible: `python run.py excel -i data.xlsx` behaves identically
to the original `python process_david_excel.py -i data.xlsx`.
"""

from __future__ import annotations

import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)
log = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Groovon Pipeline — source → enrich → sink",
    )
    sub = parser.add_subparsers(dest="source_type", help="Event source type")

    # ── Excel source (default / backward-compat) ─────────────────
    p_excel = sub.add_parser("excel", help="Read from David-format Excel")
    p_excel.add_argument("--input", "-i", required=True, help="Input .xlsx path")
    p_excel.add_argument("--output", "-o", default=None, help="Output .xlsx path")
    p_excel.add_argument("--start", "-s", type=int, default=0)
    p_excel.add_argument("--limit", "-l", type=int, default=9999)
    p_excel.add_argument("--batch-size", "-b", type=int, default=15)
    p_excel.add_argument("--dry-run", action="store_true")
    p_excel.add_argument("--json-out", default=None, help="Also dump profiles JSON")
    p_excel.add_argument("--no-supabase", action="store_true", help="Skip Supabase upload")

    # ── Scraper source ───────────────────────────────────────────
    p_scraper = sub.add_parser("scraper", help="Read from scraper JSON output")
    p_scraper.add_argument("--json", required=True, help="Scraper output JSON path")
    p_scraper.add_argument("--city", default="", help="City override")
    p_scraper.add_argument("--output", "-o", default=None, help="Output .xlsx path")
    p_scraper.add_argument("--json-out", default="profiles_scraper.json", help="Profiles JSON output")
    p_scraper.add_argument("--batch-size", "-b", type=int, default=15)
    p_scraper.add_argument("--dry-run", action="store_true")

    # ── API source ───────────────────────────────────────────────
    p_api = sub.add_parser("api", help="Fetch from Ticketmaster/SeatGeek")
    p_api.add_argument("--city", required=True, help="City to search")
    p_api.add_argument("--days", type=int, default=30, help="Days ahead")
    p_api.add_argument(
        "--genre",
        action="append",
        default=[],
        help="Genre filter (repeat or use comma-separated values)",
    )
    p_api.add_argument("--venue", default="", help="Venue name contains")
    p_api.add_argument("--artist", default="", help="Artist name contains")
    p_api.add_argument(
        "--apis",
        default="ticketmaster,seatgeek,bandsintown",
        help="Comma-separated APIs to query",
    )
    p_api.add_argument("--max-events", type=int, default=200, help="Max events per API")
    p_api.add_argument("--output", "-o", default=None, help="Output .xlsx path")
    p_api.add_argument("--json-out", default="profiles_api.json", help="Profiles JSON output")
    p_api.add_argument("--batch-size", "-b", type=int, default=15)
    p_api.add_argument("--dry-run", action="store_true")

    return parser


def main(argv: list[str] | None = None):
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.source_type:
        parser.print_help()
        sys.exit(1)

    # ── build source ─────────────────────────────────────────────
    from sources.excel_source import ExcelSource
    from sources.scraper_source import ScraperSource
    from sources.api_source import APISource
    from sinks.excel_sink import ExcelSink
    from sinks.json_sink import JsonSink
    from sinks.supabase_sink import SupabaseSink
    from engine import GroovonEngine

    source: object
    sinks: list = []

    if args.source_type == "excel":
        source = ExcelSource(config={"path": args.input})
        out_path = args.output or args.input.replace(".xlsx", "_enriched.xlsx")
        sinks.append(ExcelSink(config={"output_path": out_path, "input_path": args.input}))
        if args.json_out:
            sinks.append(JsonSink(config={"output_path": args.json_out}))
        if not getattr(args, "no_supabase", False) and not args.dry_run:
            sinks.append(SupabaseSink())

    elif args.source_type == "scraper":
        source = ScraperSource(config={"json_path": args.json, "city": args.city})
        if args.output:
            sinks.append(ExcelSink(config={"output_path": args.output}))
        sinks.append(JsonSink(config={"output_path": args.json_out}))

    elif args.source_type == "api":
        apis = [a.strip().lower() for a in args.apis.split(",") if a.strip()]
        source = APISource(
            config={
                "city": args.city,
                "days_ahead": args.days,
                "max_events": args.max_events,
                "apis": apis,
                "genres": args.genre,
                "venue": args.venue,
                "artist": args.artist,
            }
        )
        if args.output:
            sinks.append(ExcelSink(config={"output_path": args.output}))
        sinks.append(JsonSink(config={"output_path": args.json_out}))

    else:
        parser.print_help()
        sys.exit(1)

    engine = GroovonEngine(source=source, sinks=sinks, dry_run=args.dry_run)
    engine.run(
        batch_size=getattr(args, "batch_size", 15),
        start=getattr(args, "start", 0),
        limit=getattr(args, "limit", 9999),
    )


if __name__ == "__main__":
    main()
