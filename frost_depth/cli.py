from __future__ import annotations

import argparse

from .core import run_analysis_for_station, write_analysis_outputs
from .stations import find_nearby_stations, geocode_location


def _resolve_station(args: argparse.Namespace) -> tuple[str, list[str]]:
    notes: list[str] = []
    if args.location:
        location = geocode_location(args.location)
        station_search = find_nearby_stations(location.latitude, location.longitude, args.start, args.end, limit=1)
        stations = station_search.stations
        if stations.empty:
            raise RuntimeError(f"No nearby NOAA daily-summary stations found for {location.label}.")
        nearest = stations.iloc[0]
        notes.extend(station_search.warnings)
        notes.append(
            f"Resolved {args.location!r} to {location.label} via {location.source}; using nearest station "
            f"{nearest['station_id']} ({nearest['station_name']}, {nearest['distance_km']} km, {nearest['provider']})."
        )
        return f"{nearest['provider']}::{nearest['station_id']}", notes

    return f"NOAA::{args.station}", notes


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--station", default="USW00014734", help="NOAA GHCND station ID")
    parser.add_argument(
        "--location",
        help="Street address or ZIP code. If provided, the nearest NOAA station is selected automatically.",
    )
    parser.add_argument("--start", default="1995-07-01", help="Start date, YYYY-MM-DD")
    parser.add_argument("--end", default="2026-06-30", help="End date, YYYY-MM-DD")
    parser.add_argument(
        "--k-cm",
        type=float,
        default=2.0,
        help="Frost-depth coefficient in cm/sqrt(C-day)",
    )
    parser.add_argument(
        "--winter",
        default="2024-2025",
        help="Winter label to print day-by-day, e.g. 2024-2025",
    )
    parser.add_argument("--output-dir", default="output", help="Directory for CSV outputs")
    args = parser.parse_args()

    station_ref, notes = _resolve_station(args)
    provider, station_id = station_ref.split("::", 1)
    result = run_analysis_for_station(station_id, args.start, args.end, k_cm=args.k_cm, provider=provider)
    output_paths = write_analysis_outputs(result, args.output_dir)

    print()
    for note in notes:
        print(note)
    print(f"Station: {station_id} ({provider})")
    print(f"Date range: {args.start} to {args.end}")
    print(f"k_cm: {args.k_cm}")
    print()

    if result.winter_summary.empty:
        print("No October-April rows were available for the winter summary.")
    else:
        print(result.winter_summary.to_string(index=False))

    print()
    print(f"Missing NOAA days filled: {len(result.missing_days)}")
    if not result.missing_days.empty:
        display_cols = ["DATE", "PREV_OBSERVED_DATE", "NEXT_OBSERVED_DATE", "FILL_METHOD"]
        print(result.missing_days[display_cols].to_string(index=False))

    one_winter = result.daily[
        (result.daily["WINTER_LABEL"] == args.winter)
        & (result.daily["DATE"].dt.month.isin({10, 11, 12, 1, 2, 3, 4}))
    ][["DATE", "TAVG", "FREEZE_DEG", "THAW_DEG", "NET_FROST_INDEX", "DEPTH_CM", "DEPTH_IN", "TEMP_SOURCE"]]

    if not one_winter.empty:
        print()
        print(f"Daily values for winter {args.winter}:")
        print(one_winter.to_string(index=False))
    else:
        print()
        print(f"No rows found for winter {args.winter}")

    if result.warnings:
        print()
        print("Warnings:")
        for warning in result.warnings:
            print(f"- {warning}")

    print()
    print(f"Wrote {output_paths['daily']}")
    print(f"Wrote {output_paths['summary']}")
    print(f"Wrote {output_paths['missing_days']}")
