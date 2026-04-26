from __future__ import annotations

from dataclasses import dataclass
import math
import re

from meteostat import Stations
import pandas as pd
import pgeocode

from .http import build_session, get_json

NOAA_SEARCH_URL = "https://www.ncei.noaa.gov/access/services/search/v1/data"
CENSUS_GEOCODER_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
ZIP_PATTERN = re.compile(r"^\d{5}(?:-\d{4})?$")
HTTP = build_session()


@dataclass
class LocationResult:
    query: str
    latitude: float
    longitude: float
    label: str
    source: str


@dataclass
class StationSearchResult:
    stations: pd.DataFrame
    warnings: list[str]


def geocode_location(query: str) -> LocationResult:
    cleaned = query.strip()
    if not cleaned:
        raise ValueError("Enter a ZIP code or street address.")

    if ZIP_PATTERN.fullmatch(cleaned):
        nominatim = pgeocode.Nominatim("us")
        zip5 = cleaned[:5]
        location = nominatim.query_postal_code(zip5)
        if pd.isna(location.latitude) or pd.isna(location.longitude):
            raise ValueError(f"Could not find coordinates for ZIP code {zip5}.")
        place_name = ", ".join(part for part in [location.place_name, location.state_name] if isinstance(part, str))
        return LocationResult(
            query=cleaned,
            latitude=float(location.latitude),
            longitude=float(location.longitude),
            label=place_name or f"ZIP {zip5}",
            source="ZIP centroid",
        )

    params = {
        "address": cleaned,
        "benchmark": "Public_AR_Current",
        "format": "json",
    }
    payload = get_json(
        HTTP,
        CENSUS_GEOCODER_URL,
        params=params,
        timeout=60,
        service_name="the U.S. Census geocoder",
    )
    matches = payload.get("result", {}).get("addressMatches", [])
    if not matches:
        raise ValueError("Could not geocode that address. Try a fuller street address or a ZIP code.")

    best_match = matches[0]
    coordinates = best_match["coordinates"]
    return LocationResult(
        query=cleaned,
        latitude=float(coordinates["y"]),
        longitude=float(coordinates["x"]),
        label=best_match["matchedAddress"],
        source="U.S. Census geocoder",
    )


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    a_value = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    )
    c_value = 2 * math.atan2(math.sqrt(a_value), math.sqrt(1 - a_value))
    return radius_km * c_value


def _bbox_for_radius(latitude: float, longitude: float, radius_km: float) -> str:
    lat_delta = radius_km / 111.0
    lon_delta = radius_km / max(1e-6, 111.0 * math.cos(math.radians(latitude)))
    north = latitude + lat_delta
    south = latitude - lat_delta
    east = longitude + lon_delta
    west = longitude - lon_delta
    return f"{north:.4f},{west:.4f},{south:.4f},{east:.4f}"


def _empty_station_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "station_id",
            "station_name",
            "distance_km",
            "latitude",
            "longitude",
            "has_tavg",
            "has_tmax_tmin",
            "tavg_coverage_pct",
            "tavg_start",
            "tavg_end",
            "platforms",
            "provider",
        ]
    )


def _find_nearby_noaa_stations(
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
    limit: int = 12,
) -> pd.DataFrame:
    station_rows: dict[str, dict[str, object]] = {}

    for radius_km in [25, 50, 100, 200, 400]:
        params = {
            "dataset": "daily-summaries",
            "bbox": _bbox_for_radius(latitude, longitude, radius_km),
            "startDate": start_date,
            "endDate": end_date,
        }
        payload = get_json(
            HTTP,
            NOAA_SEARCH_URL,
            params=params,
            timeout=90,
            service_name="NOAA station search",
        )

        for result in payload.get("results", []):
            station_entries = result.get("stations", [])
            if not station_entries:
                continue

            point = result.get("boundingPoints", [{}])[0].get("point")
            if not point or len(point) != 2:
                continue

            station_lon, station_lat = float(point[0]), float(point[1])
            for station in station_entries:
                station_id = station["id"]
                data_types = {item["id"]: item for item in station.get("dataTypes", [])}
                supports_temperature = "TAVG" in data_types or ("TMAX" in data_types and "TMIN" in data_types)
                if not supports_temperature:
                    continue

                if station_id not in station_rows:
                    tavg_meta = data_types.get("TAVG")
                    station_rows[station_id] = {
                        "station_id": station_id,
                        "station_name": station.get("name", station_id),
                        "latitude": station_lat,
                        "longitude": station_lon,
                        "distance_km": round(_haversine_km(latitude, longitude, station_lat, station_lon), 1),
                        "has_tavg": "TAVG" in data_types,
                        "has_tmax_tmin": "TMAX" in data_types and "TMIN" in data_types,
                        "tavg_coverage_pct": round(float(tavg_meta["coverage"]), 1) if tavg_meta else None,
                        "tavg_start": tavg_meta["dateRange"]["start"][:10] if tavg_meta else "",
                        "tavg_end": tavg_meta["dateRange"]["end"][:10] if tavg_meta else "",
                        "platforms": ", ".join(platform["id"] for platform in station.get("platforms", [])),
                        "provider": "NOAA",
                    }

        if len(station_rows) >= limit:
            break

    if not station_rows:
        return _empty_station_frame()

    return (
        pd.DataFrame(station_rows.values())
        .sort_values(["distance_km", "station_name"])
        .head(limit)
        .reset_index(drop=True)
    )


def _find_nearby_meteostat_stations(
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
    limit: int = 12,
) -> pd.DataFrame:
    stations = Stations().nearby(latitude, longitude).fetch(limit * 3).reset_index()
    if stations.empty:
        return _empty_station_frame()

    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    stations = stations[
        stations["daily_start"].notna()
        & stations["daily_end"].notna()
        & (stations["daily_start"] <= end_ts)
        & (stations["daily_end"] >= start_ts)
    ].copy()

    if stations.empty:
        return _empty_station_frame()

    stations["distance_km"] = (stations["distance"] / 1000).round(1)
    stations["has_tavg"] = True
    stations["has_tmax_tmin"] = True
    stations["tavg_coverage_pct"] = None
    stations["tavg_start"] = stations["daily_start"].dt.strftime("%Y-%m-%d")
    stations["tavg_end"] = stations["daily_end"].dt.strftime("%Y-%m-%d")
    stations["platforms"] = "Meteostat"
    stations["provider"] = "Meteostat"

    return (
        stations.rename(
            columns={
                "id": "station_id",
                "name": "station_name",
            }
        )[
            [
                "station_id",
                "station_name",
                "distance_km",
                "latitude",
                "longitude",
                "has_tavg",
                "has_tmax_tmin",
                "tavg_coverage_pct",
                "tavg_start",
                "tavg_end",
                "platforms",
                "provider",
            ]
        ]
        .sort_values(["distance_km", "station_name"])
        .head(limit)
        .reset_index(drop=True)
    )


def find_nearby_stations(
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
    limit: int = 12,
) -> StationSearchResult:
    try:
        stations = _find_nearby_noaa_stations(latitude, longitude, start_date, end_date, limit=limit)
        return StationSearchResult(stations=stations, warnings=[])
    except RuntimeError:
        fallback = _find_nearby_meteostat_stations(latitude, longitude, start_date, end_date, limit=limit)
        warnings = []
        if not fallback.empty:
            warnings.append(
                "NOAA station search is temporarily unavailable, so the station list is coming from Meteostat."
            )
        return StationSearchResult(stations=fallback, warnings=warnings)
