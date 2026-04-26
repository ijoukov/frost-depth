# Frost depth estimate from NOAA daily temperatures

This project fetches NOAA daily summaries and estimates frost depth using a simple net frost index model. It now supports both:

- a command line interface
- a Dash web app with station lookup, charts, warnings, and interactive tables

Default station:

- `USW00014734` — Newark Liberty International Airport, NJ

## Install

```bash
make install
```

That creates a local `.venv` and installs all dependencies into it.

If you want to create the environment first, you can also run:

```bash
make venv
make install
```

## Run default analysis

```bash
make run
```

```bash
.venv/bin/python frost_depth.py
```

## Run just the 2024-2025 winter

```bash
make run-2024
```

```bash
.venv/bin/python frost_depth.py --start 2024-07-01 --end 2025-06-30 --winter 2024-2025
```

## Run using a ZIP code or address

```bash
.venv/bin/python frost_depth.py --location 07114 --start 2020-07-01 --end 2025-06-30
```

When `--location` is provided, the CLI geocodes the location and automatically picks the nearest NOAA daily-summary station it finds for the selected date range.

## Run the web app

```bash
make run-web
```

Then open `http://127.0.0.1:8050/`

## Outputs

The CLI writes:

- `output/daily_frost_depth.csv`
- `output/winter_summary.csv`
- `output/missing_days.csv`

The web app shows:

- nearby-station search from address or ZIP code
- sortable/filterable AG Grid tables
- a line chart of frost depth over time
- a bar chart of winter maximum frost depth
- a warnings area for missing days and other issues

## Model

Daily values:

```text
freeze_deg = max(0, -Tavg_C)
thaw_deg   = max(0,  Tavg_C)
net_frost_index_today = max(0, net_frost_index_yesterday + freeze_deg - thaw_deg)
depth_cm = k_cm * sqrt(net_frost_index)
```

Default:

```text
k_cm = 2.0 cm / sqrt(C-day)
```

That coefficient is a rough value for frost-susceptible wetter soil. It is not a substitute for a proper soil heat-transfer model.

## Notes

- NOAA sometimes does not return `TAVG`. The script fills it using `(TMAX + TMIN) / 2` when possible.
- ZIP code lookup uses the ZIP centroid, while street-address lookup uses the U.S. Census geocoder.
- Nearby stations are discovered with NOAA's search API for the chosen date range, then the selected station is analyzed through NOAA daily summaries.
- Missing calendar days in the requested date range are detected, filled from neighboring observed days using linear interpolation, and written to `output/missing_days.csv`.
- If a missing run touches the requested start or end date, the fill falls back to the nearest observed edge value.
- Winter seasons are grouped July-June and labeled like `2024-2025`.
- Frost months used for the summary are October through April.
