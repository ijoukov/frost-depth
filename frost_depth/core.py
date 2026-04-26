from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.error import URLError

from meteostat import Daily
import pandas as pd

from .http import build_session, get_json

NOAA_DAILY_URL = "https://www.ncei.noaa.gov/access/services/data/v1"
HTTP = build_session()


@dataclass
class AnalysisResult:
    station_id: str
    provider: str
    daily: pd.DataFrame
    winter_summary: pd.DataFrame
    missing_days: pd.DataFrame
    warnings: list[str]


def fetch_noaa_daily_summaries(
    station: str,
    start_date: str,
    end_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    params = {
        "dataset": "daily-summaries",
        "stations": station,
        "startDate": start_date,
        "endDate": end_date,
        "dataTypes": "TAVG,TMAX,TMIN",
        "units": "metric",
        "format": "json",
    }

    rows = get_json(
        HTTP,
        NOAA_DAILY_URL,
        params=params,
        timeout=90,
        service_name="NOAA daily summaries",
    )

    if not rows:
        raise RuntimeError("NOAA returned no rows for that station/date range.")

    df = pd.DataFrame(rows)
    df.columns = [column.upper() for column in df.columns]

    if "DATE" not in df.columns:
        raise RuntimeError(f"NOAA response did not include DATE. Columns: {list(df.columns)}")

    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")

    for column in ["TAVG", "TMAX", "TMIN"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    if "TAVG" not in df.columns:
        df["TAVG"] = pd.NA

    df = df.dropna(subset=["DATE"]).sort_values("DATE").reset_index(drop=True)
    if df.empty:
        raise RuntimeError("No usable temperature rows after parsing the NOAA response.")

    original_tavg_missing = df["TAVG"].isna()
    midpoint_dates = pd.Index(df.loc[original_tavg_missing, "DATE"]).dropna()
    if "TMAX" in df.columns and "TMIN" in df.columns:
        can_fill_tavg = df["TMAX"].notna() & df["TMIN"].notna()
        df.loc[original_tavg_missing & can_fill_tavg, "TAVG"] = (
            df.loc[original_tavg_missing & can_fill_tavg, "TMAX"]
            + df.loc[original_tavg_missing & can_fill_tavg, "TMIN"]
        ) / 2

    requested_dates = pd.date_range(start=start_date, end=end_date, freq="D")
    df = (
        df.drop_duplicates(subset=["DATE"], keep="last")
        .set_index("DATE")
        .reindex(requested_dates)
        .rename_axis("DATE")
        .reset_index()
    )

    df["STATION"] = df["STATION"].fillna(station)
    df["TEMP_SOURCE"] = "observed_tavg"

    midpoint_mask = df["DATE"].isin(midpoint_dates) & df["TAVG"].notna()
    df.loc[midpoint_mask, "TEMP_SOURCE"] = "estimated_from_tmax_tmin"

    missing_date_mask = df["TAVG"].isna()
    df.loc[missing_date_mask, "TEMP_SOURCE"] = "estimated_from_neighbors"

    for column in ["TAVG", "TMAX", "TMIN"]:
        if column not in df.columns:
            df[column] = pd.NA
        df[column] = pd.to_numeric(df[column], errors="coerce")
        df[column] = df[column].interpolate(method="linear", limit_direction="both")

    if df["TAVG"].isna().all():
        raise RuntimeError("No usable temperature rows after filling missing days.")

    missing_days = df.loc[missing_date_mask, ["DATE"]].copy()
    missing_days["STATION"] = station
    missing_days["PREV_OBSERVED_DATE"] = pd.NaT
    missing_days["NEXT_OBSERVED_DATE"] = pd.NaT

    observed_dates = df.loc[~missing_date_mask, "DATE"]
    for index, missing_date in missing_days["DATE"].items():
        previous_dates = observed_dates[observed_dates < missing_date]
        next_dates = observed_dates[observed_dates > missing_date]
        missing_days.at[index, "PREV_OBSERVED_DATE"] = (
            previous_dates.iloc[-1] if not previous_dates.empty else pd.NaT
        )
        missing_days.at[index, "NEXT_OBSERVED_DATE"] = (
            next_dates.iloc[0] if not next_dates.empty else pd.NaT
        )

    missing_days["FILL_METHOD"] = "linear_interpolation"
    edge_mask = missing_days["PREV_OBSERVED_DATE"].isna() | missing_days["NEXT_OBSERVED_DATE"].isna()
    missing_days.loc[edge_mask, "FILL_METHOD"] = "nearest_edge_value"

    df = df.dropna(subset=["TAVG"]).reset_index(drop=True)
    missing_days = missing_days.reset_index(drop=True)

    return df, missing_days


def fetch_meteostat_daily_summaries(
    station: str,
    start_date: str,
    end_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    try:
        data = Daily(station, start=pd.Timestamp(start_date), end=pd.Timestamp(end_date)).fetch()
    except URLError as exc:
        raise RuntimeError(
            "Could not reach Meteostat daily data. This is usually a temporary network or DNS issue. "
            "Please try again in a moment."
        ) from exc

    if data.empty:
        raise RuntimeError("Meteostat returned no rows for that station/date range.")

    df = data.reset_index().rename(
        columns={
            "time": "DATE",
            "tavg": "TAVG",
            "tmin": "TMIN",
            "tmax": "TMAX",
        }
    )
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    df["STATION"] = station

    for column in ["TAVG", "TMAX", "TMIN"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    original_tavg_missing = df["TAVG"].isna()
    midpoint_dates = pd.Index(df.loc[original_tavg_missing, "DATE"]).dropna()
    can_fill_tavg = df["TMAX"].notna() & df["TMIN"].notna()
    df.loc[original_tavg_missing & can_fill_tavg, "TAVG"] = (
        df.loc[original_tavg_missing & can_fill_tavg, "TMAX"]
        + df.loc[original_tavg_missing & can_fill_tavg, "TMIN"]
    ) / 2

    requested_dates = pd.date_range(start=start_date, end=end_date, freq="D")
    df = (
        df.drop_duplicates(subset=["DATE"], keep="last")
        .set_index("DATE")
        .reindex(requested_dates)
        .rename_axis("DATE")
        .reset_index()
    )

    df["STATION"] = df["STATION"].fillna(station)
    df["TEMP_SOURCE"] = "observed_tavg"
    midpoint_mask = df["DATE"].isin(midpoint_dates) & df["TAVG"].notna()
    df.loc[midpoint_mask, "TEMP_SOURCE"] = "estimated_from_tmax_tmin"

    missing_date_mask = df["TAVG"].isna()
    df.loc[missing_date_mask, "TEMP_SOURCE"] = "estimated_from_neighbors"

    for column in ["TAVG", "TMAX", "TMIN"]:
        if column not in df.columns:
            df[column] = pd.NA
        df[column] = pd.to_numeric(df[column], errors="coerce")
        df[column] = df[column].interpolate(method="linear", limit_direction="both")

    if df["TAVG"].isna().all():
        raise RuntimeError("No usable Meteostat temperature rows after filling missing days.")

    missing_days = df.loc[missing_date_mask, ["DATE"]].copy()
    missing_days["STATION"] = station
    missing_days["PREV_OBSERVED_DATE"] = pd.NaT
    missing_days["NEXT_OBSERVED_DATE"] = pd.NaT

    observed_dates = df.loc[~missing_date_mask, "DATE"]
    for index, missing_date in missing_days["DATE"].items():
        previous_dates = observed_dates[observed_dates < missing_date]
        next_dates = observed_dates[observed_dates > missing_date]
        missing_days.at[index, "PREV_OBSERVED_DATE"] = (
            previous_dates.iloc[-1] if not previous_dates.empty else pd.NaT
        )
        missing_days.at[index, "NEXT_OBSERVED_DATE"] = (
            next_dates.iloc[0] if not next_dates.empty else pd.NaT
        )

    missing_days["FILL_METHOD"] = "linear_interpolation"
    edge_mask = missing_days["PREV_OBSERVED_DATE"].isna() | missing_days["NEXT_OBSERVED_DATE"].isna()
    missing_days.loc[edge_mask, "FILL_METHOD"] = "nearest_edge_value"

    df = df.dropna(subset=["TAVG"]).reset_index(drop=True)
    missing_days = missing_days.reset_index(drop=True)

    return df, missing_days


def add_frost_depth_columns(df: pd.DataFrame, k_cm: float) -> pd.DataFrame:
    enriched = df.copy()

    enriched["WINTER_YEAR"] = enriched["DATE"].apply(
        lambda date_value: date_value.year if date_value.month >= 7 else date_value.year - 1
    )
    enriched["FREEZE_DEG"] = (-enriched["TAVG"]).clip(lower=0)
    enriched["THAW_DEG"] = enriched["TAVG"].clip(lower=0)

    def compute_net_frost(group: pd.DataFrame) -> pd.DataFrame:
        ordered = group.sort_values("DATE").copy()
        storage = 0.0
        net_values: list[float] = []

        for _, row in ordered.iterrows():
            storage += float(row["FREEZE_DEG"])
            storage -= float(row["THAW_DEG"])
            storage = max(0.0, storage)
            net_values.append(storage)

        ordered["NET_FROST_INDEX"] = net_values
        ordered["DEPTH_CM"] = k_cm * ordered["NET_FROST_INDEX"].pow(0.5)
        ordered["DEPTH_IN"] = ordered["DEPTH_CM"] / 2.54
        return ordered

    winter_frames = []
    for winter_year, group in enriched.groupby("WINTER_YEAR", sort=True):
        computed = compute_net_frost(group)
        computed["WINTER_LABEL"] = f"{winter_year}-{winter_year + 1}"
        winter_frames.append(computed)

    if not winter_frames:
        return pd.DataFrame(
            columns=list(enriched.columns) + ["NET_FROST_INDEX", "DEPTH_CM", "DEPTH_IN", "WINTER_LABEL"]
        )

    return pd.concat(winter_frames, ignore_index=True)


def summarize_by_winter(df: pd.DataFrame, frost_months: set[int]) -> pd.DataFrame:
    df_frost = df[df["DATE"].dt.month.isin(frost_months)].copy()
    summary_columns = [
        "WINTER",
        "MAX_DATE",
        "MAX_NET_FROST_INDEX_C_DAYS",
        "MAX_DEPTH_CM",
        "MAX_DEPTH_IN",
    ]

    summary_rows = []
    for winter_label, group in df_frost.groupby("WINTER_LABEL"):
        max_idx = group["DEPTH_CM"].idxmax()
        max_row = group.loc[max_idx]
        summary_rows.append(
            {
                "WINTER": winter_label,
                "MAX_DATE": max_row["DATE"].date().isoformat(),
                "MAX_NET_FROST_INDEX_C_DAYS": round(float(max_row["NET_FROST_INDEX"]), 1),
                "MAX_DEPTH_CM": round(float(max_row["DEPTH_CM"]), 1),
                "MAX_DEPTH_IN": round(float(max_row["DEPTH_IN"]), 1),
            }
        )

    if not summary_rows:
        return pd.DataFrame(columns=summary_columns)

    return pd.DataFrame(summary_rows, columns=summary_columns).sort_values("WINTER").reset_index(drop=True)


def build_warning_messages(missing_days: pd.DataFrame, winter_summary: pd.DataFrame) -> list[str]:
    warnings: list[str] = []

    if not missing_days.empty:
        missing_list = ", ".join(missing_days["DATE"].dt.strftime("%Y-%m-%d").tolist())
        warnings.append(
            f"Filled {len(missing_days)} missing NOAA day(s) by interpolation or edge carry-forward: {missing_list}."
        )

    if winter_summary.empty:
        warnings.append("No October-April rows were available, so the winter summary is empty.")

    return warnings


def run_analysis_for_station(
    station_id: str,
    start_date: str,
    end_date: str,
    k_cm: float = 2.0,
    provider: str = "NOAA",
) -> AnalysisResult:
    provider_normalized = provider.lower()
    if provider_normalized == "meteostat":
        daily_raw, missing_days = fetch_meteostat_daily_summaries(station_id, start_date, end_date)
    else:
        daily_raw, missing_days = fetch_noaa_daily_summaries(station_id, start_date, end_date)

    daily = add_frost_depth_columns(daily_raw, k_cm)
    winter_summary = summarize_by_winter(daily, frost_months={10, 11, 12, 1, 2, 3, 4})
    warnings = build_warning_messages(missing_days, winter_summary)
    if provider_normalized == "meteostat":
        warnings.insert(0, "Using Meteostat station data because NOAA search was unavailable.")
    return AnalysisResult(
        station_id=station_id,
        provider=provider,
        daily=daily,
        winter_summary=winter_summary,
        missing_days=missing_days,
        warnings=warnings,
    )


def write_analysis_outputs(result: AnalysisResult, output_dir: str | Path) -> dict[str, Path]:
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    paths = {
        "daily": target_dir / "daily_frost_depth.csv",
        "summary": target_dir / "winter_summary.csv",
        "missing_days": target_dir / "missing_days.csv",
    }

    result.daily.to_csv(paths["daily"], index=False)
    result.winter_summary.to_csv(paths["summary"], index=False)
    result.missing_days.to_csv(paths["missing_days"], index=False)

    return paths
