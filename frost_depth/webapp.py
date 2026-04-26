from __future__ import annotations

from datetime import date
import os
import site
import sys
import types

def _prefer_active_environment_packages() -> None:
    user_site = site.getusersitepackages()
    if os.environ.get("VIRTUAL_ENV") or os.environ.get("CONDA_PREFIX"):
        remaining_paths = [path for path in sys.path if path != user_site]
        if user_site in sys.path:
            remaining_paths.append(user_site)
        sys.path[:] = remaining_paths


_prefer_active_environment_packages()

import pandas as pd

from .core import run_analysis_for_station
from .stations import find_nearby_stations, geocode_location


def _install_dash_comm_shim() -> None:
    try:
        from comm import create_comm

        create_comm(target_name="dash")
    except NotImplementedError:
        shim = types.ModuleType("comm")
        shim.create_comm = lambda *args, **kwargs: None
        sys.modules["comm"] = shim
    except ImportError:
        pass


_install_dash_comm_shim()

from dash import Dash, Input, Output, State, dcc, html
import dash_ag_grid as dag
import plotly.express as px


def _serialize_frame(frame: pd.DataFrame) -> list[dict[str, object]]:
    if frame.empty:
        return []

    serialized = frame.copy()
    for column in serialized.columns:
        if pd.api.types.is_datetime64_any_dtype(serialized[column]):
            serialized[column] = serialized[column].dt.strftime("%Y-%m-%d")
    return serialized.where(pd.notnull(serialized), None).to_dict("records")


def _warning_cards(messages: list[str], tone: str = "warn") -> list[html.Div]:
    if not messages:
        return [html.Div("No warnings right now.", className="notice notice-ok")]
    return [html.Div(message, className=f"notice notice-{tone}") for message in messages]


def _empty_figure(message: str):
    figure = px.line()
    figure.update_layout(
        template="plotly_white",
        xaxis={"visible": False},
        yaxis={"visible": False},
        annotations=[
            {
                "text": message,
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {"size": 16, "color": "#4f5d75"},
            }
        ],
    )
    return figure


def _depth_figure(daily_records: list[dict[str, object]], unit: str):
    if not daily_records:
        return _empty_figure("Run an analysis to see frost depth over time.")

    frame = pd.DataFrame(daily_records)
    y_field = "DEPTH_CM" if unit == "cm" else "DEPTH_IN"
    y_label = "Depth (cm)" if unit == "cm" else "Depth (in)"
    figure = px.line(
        frame,
        x="DATE",
        y=y_field,
        template="plotly_white",
        markers=False,
        title="Estimated Frost Depth Over Time",
    )
    figure.update_traces(line={"color": "#2f6fef", "width": 3})
    figure.update_layout(margin={"l": 30, "r": 20, "t": 60, "b": 30})
    figure.update_yaxes(title_text=y_label)
    figure.update_xaxes(title_text="")
    return figure


def _winter_bar_figure(summary_records: list[dict[str, object]], unit: str):
    if not summary_records:
        return _empty_figure("Winter maximums will appear here once summary rows exist.")

    frame = pd.DataFrame(summary_records)
    y_field = "MAX_DEPTH_CM" if unit == "cm" else "MAX_DEPTH_IN"
    y_label = "Max depth (cm)" if unit == "cm" else "Max depth (in)"
    figure = px.bar(
        frame,
        x="WINTER",
        y=y_field,
        template="plotly_white",
        title="Maximum Frost Depth by Winter",
        color=y_field,
        color_continuous_scale=["#d9ecff", "#6fa8ff", "#1d4ed8"],
    )
    figure.update_layout(margin={"l": 30, "r": 20, "t": 60, "b": 30}, coloraxis_showscale=False)
    figure.update_yaxes(title_text=y_label)
    figure.update_xaxes(title_text="")
    return figure


def _default_col_def() -> dict[str, object]:
    return {
        "sortable": True,
        "filter": True,
        "floatingFilter": True,
        "resizable": True,
        "minWidth": 110,
    }


STATION_COLUMNS = [
    {"field": "station_id", "headerName": "Station ID", "checkboxSelection": True},
    {"field": "station_name", "headerName": "Station"},
    {"field": "provider", "headerName": "Provider"},
    {"field": "distance_km", "headerName": "Distance (km)", "type": "numericColumn"},
    {"field": "has_tavg", "headerName": "Has TAVG"},
    {"field": "has_tmax_tmin", "headerName": "Has TMAX/TMIN"},
    {"field": "tavg_coverage_pct", "headerName": "TAVG Coverage %", "type": "numericColumn"},
    {"field": "tavg_start", "headerName": "TAVG Start"},
    {"field": "tavg_end", "headerName": "TAVG End"},
    {"field": "platforms", "headerName": "Platforms"},
]

MISSING_COLUMNS = [
    {"field": "DATE", "headerName": "Missing Date"},
    {"field": "PREV_OBSERVED_DATE", "headerName": "Previous Observed"},
    {"field": "NEXT_OBSERVED_DATE", "headerName": "Next Observed"},
    {"field": "FILL_METHOD", "headerName": "Fill Method"},
]


def _summary_columns(unit: str) -> list[dict[str, object]]:
    depth_field = "MAX_DEPTH_CM" if unit == "cm" else "MAX_DEPTH_IN"
    depth_label = "Max Depth (cm)" if unit == "cm" else "Max Depth (in)"
    return [
        {"field": "WINTER", "headerName": "Winter"},
        {"field": "MAX_DATE", "headerName": "Max Date"},
        {"field": "MAX_NET_FROST_INDEX_C_DAYS", "headerName": "Net Frost Index", "type": "numericColumn"},
        {"field": depth_field, "headerName": depth_label, "type": "numericColumn"},
    ]


def _daily_columns(unit: str) -> list[dict[str, object]]:
    depth_field = "DEPTH_CM" if unit == "cm" else "DEPTH_IN"
    depth_label = "Depth (cm)" if unit == "cm" else "Depth (in)"
    return [
        {"field": "DATE", "headerName": "Date"},
        {"field": "WINTER_LABEL", "headerName": "Winter"},
        {"field": "TAVG", "headerName": "TAVG C", "type": "numericColumn"},
        {"field": "FREEZE_DEG", "headerName": "Freeze Deg", "type": "numericColumn"},
        {"field": "THAW_DEG", "headerName": "Thaw Deg", "type": "numericColumn"},
        {"field": "NET_FROST_INDEX", "headerName": "Net Frost Index", "type": "numericColumn"},
        {"field": depth_field, "headerName": depth_label, "type": "numericColumn"},
        {"field": "TEMP_SOURCE", "headerName": "Temperature Source"},
    ]


def create_app() -> Dash:
    app = Dash(__name__, title="Frost Depth Explorer")

    today = date.today()

    app.layout = html.Div(
        className="page-shell",
        children=[
            dcc.Store(id="analysis-store"),
            html.Div(
                className="hero-card",
                children=[
                    html.Div("Frost Depth Explorer", className="eyebrow"),
                    html.H1("Estimate real winter frost depth from NOAA temperatures."),
                    html.P(
                        "Pick a location, find a nearby station, and explore daily frost depth, "
                        "winter peaks, missing data warnings, and the raw modeled series."
                    ),
                ],
            ),
            html.Div(
                className="content-grid",
                children=[
                    html.Div(
                        className="control-card",
                        children=[
                            html.H2("Inputs"),
                            html.Label("Street address or ZIP code", className="field-label"),
                            dcc.Input(
                                id="location-query",
                                type="text",
                                value="07114",
                                debounce=True,
                                placeholder="Example: 07114 or 1 World Trade Center, New York, NY",
                                className="text-input",
                            ),
                            html.Label("Units", className="field-label"),
                            dcc.RadioItems(
                                id="unit-toggle",
                                options=[
                                    {"label": "Centimeters", "value": "cm"},
                                    {"label": "Inches", "value": "in"},
                                ],
                                value="cm",
                                inline=True,
                                className="unit-toggle",
                            ),
                            html.Div(
                                className="date-grid",
                                children=[
                                    html.Div(
                                        children=[
                                            html.Label("Start date", className="field-label"),
                                            dcc.Input(id="start-date", type="date", value="2024-07-01", className="date-input"),
                                        ]
                                    ),
                                    html.Div(
                                        children=[
                                            html.Label("End date", className="field-label"),
                                            dcc.Input(id="end-date", type="date", value=today.isoformat(), className="date-input"),
                                        ]
                                    ),
                                ],
                            ),
                            html.Label("k_cm coefficient", className="field-label"),
                            dcc.Input(id="k-cm", type="number", value=2.0, step=0.1, className="text-input"),
                            html.Div(
                                className="button-row",
                                children=[
                                    html.Button("Find Stations", id="find-stations", n_clicks=0, className="primary-button"),
                                    html.Button("Run Analysis", id="run-analysis", n_clicks=0, className="secondary-button"),
                                ],
                            ),
                            html.Div(id="location-summary", className="location-summary"),
                            html.Details(
                                id="station-details",
                                className="station-details",
                                children=[
                                    html.Summary("Station results"),
                                    dcc.Loading(
                                        type="default",
                                        children=html.Div(
                                            className="grid-card compact-grid",
                                            children=[
                                                html.Div(className="section-title", children="Nearby Stations"),
                                                dag.AgGrid(
                                                    id="station-grid",
                                                    columnDefs=STATION_COLUMNS,
                                                    rowData=[],
                                                    defaultColDef=_default_col_def(),
                                                    dashGridOptions={
                                                        "rowSelection": "single",
                                                        "animateRows": False,
                                                        "pagination": True,
                                                        "paginationPageSize": 8,
                                                    },
                                                    className="ag-theme-alpine frost-grid",
                                                ),
                                            ],
                                        ),
                                    ),
                                ],
                            ),
                        ],
                    ),
                    html.Div(
                        className="stack",
                        children=[
                            html.Div(
                                className="warning-card",
                                children=[
                                    html.H2("Warnings and Notes"),
                                    dcc.Loading(
                                        type="default",
                                        children=html.Div(id="warning-area", children=_warning_cards([])),
                                    ),
                                ],
                            ),
                            dcc.Loading(
                                type="default",
                                children=html.Div(
                                    className="chart-grid",
                                    children=[
                                        dcc.Graph(id="depth-chart", figure=_empty_figure("Run an analysis to see frost depth over time.")),
                                        dcc.Graph(id="winter-chart", figure=_empty_figure("Winter maximums will appear here.")),
                                    ],
                                ),
                            ),
                        ],
                    ),
                ],
            ),
            html.Div(
                className="grid-stack",
                children=[
                    html.Div(
                        className="grid-card",
                        children=[
                            html.Div(className="section-title", children="Winter Summary"),
                            dcc.Loading(
                                type="default",
                                children=dag.AgGrid(
                                    id="summary-grid",
                                    columnDefs=_summary_columns("cm"),
                                    rowData=[],
                                    defaultColDef=_default_col_def(),
                                    dashGridOptions={"pagination": True, "paginationPageSize": 10},
                                    className="ag-theme-alpine frost-grid",
                                ),
                            ),
                        ],
                    ),
                    html.Div(
                        className="grid-card",
                        children=[
                            html.Div(className="section-title", children="Missing Day Details"),
                            dcc.Loading(
                                type="default",
                                children=dag.AgGrid(
                                    id="missing-grid",
                                    columnDefs=MISSING_COLUMNS,
                                    rowData=[],
                                    defaultColDef=_default_col_def(),
                                    dashGridOptions={"pagination": True, "paginationPageSize": 10},
                                    className="ag-theme-alpine frost-grid",
                                ),
                            ),
                        ],
                    ),
                    html.Div(
                        className="grid-card",
                        children=[
                            html.Div(className="section-title", children="Daily Results"),
                            dcc.Loading(
                                type="default",
                                children=dag.AgGrid(
                                    id="daily-grid",
                                    columnDefs=_daily_columns("cm"),
                                    rowData=[],
                                    defaultColDef=_default_col_def(),
                                    dashGridOptions={"pagination": True, "paginationPageSize": 25},
                                    className="ag-theme-alpine frost-grid tall-grid",
                                ),
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )

    @app.callback(
        Output("station-grid", "rowData"),
        Output("station-grid", "selectedRows"),
        Output("location-summary", "children"),
        Output("station-details", "open"),
        Output("warning-area", "children", allow_duplicate=True),
        Input("find-stations", "n_clicks"),
        State("location-query", "value"),
        State("start-date", "date"),
        State("end-date", "date"),
        prevent_initial_call=True,
    )
    def load_stations(n_clicks: int, location_query: str, start_date: str, end_date: str):
        del n_clicks
        try:
            location = geocode_location(location_query or "")
            station_search = find_nearby_stations(location.latitude, location.longitude, start_date, end_date)
            stations = station_search.stations
        except Exception as exc:
            return [], [], "", True, _warning_cards([str(exc)], tone="error")

        if stations.empty:
            messages = [
                f"Resolved {location.label} ({location.source}), but no nearby stations with daily temperature coverage were found in that date range."
            ]
            return [], [], location.label, True, _warning_cards(messages)

        station_records = _serialize_frame(stations)
        nearest = station_records[0]
        summary_text = (
            f"{location.label} via {location.source}. "
            f"Showing the nearest stations with temperature support for {start_date} through {end_date}."
        )
        notes = [f"Selected the nearest station by default: {nearest['station_id']} ({nearest['station_name']})."]
        notes.extend(station_search.warnings)
        return station_records, [nearest], summary_text, True, _warning_cards(
            notes,
            tone="warn" if station_search.warnings else "ok",
        )

    @app.callback(
        Output("analysis-store", "data"),
        Output("warning-area", "children"),
        Input("run-analysis", "n_clicks"),
        State("station-grid", "selectedRows"),
        State("start-date", "date"),
        State("end-date", "date"),
        State("k-cm", "value"),
        prevent_initial_call=True,
    )
    def run_analysis(n_clicks: int, selected_rows: list[dict[str, object]], start_date: str, end_date: str, k_cm: float):
        del n_clicks
        if not selected_rows:
            return (
                None,
                _warning_cards(["Choose a nearby station before running the analysis."], tone="error"),
            )

        station_id = str(selected_rows[0]["station_id"])
        provider = str(selected_rows[0].get("provider", "NOAA"))

        try:
            result = run_analysis_for_station(station_id, start_date, end_date, float(k_cm), provider=provider)
        except Exception as exc:
            return (
                None,
                _warning_cards([f"Analysis failed for {station_id} ({provider}): {exc}"], tone="error"),
            )

        messages = [f"Analysis ran for station {station_id} using {provider} data."]
        messages.extend(result.warnings)

        return (
            {
                "summary": _serialize_frame(result.winter_summary),
                "daily": _serialize_frame(result.daily),
                "missing": _serialize_frame(result.missing_days),
            },
            _warning_cards(messages, tone="warn" if result.warnings else "ok"),
        )

    @app.callback(
        Output("summary-grid", "rowData"),
        Output("summary-grid", "columnDefs"),
        Output("daily-grid", "rowData"),
        Output("daily-grid", "columnDefs"),
        Output("missing-grid", "rowData"),
        Output("depth-chart", "figure"),
        Output("winter-chart", "figure"),
        Input("analysis-store", "data"),
        Input("unit-toggle", "value"),
    )
    def render_analysis(analysis_data: dict[str, object] | None, unit: str):
        if not analysis_data:
            return (
                [],
                _summary_columns(unit),
                [],
                _daily_columns(unit),
                [],
                _empty_figure("Run an analysis to see frost depth over time."),
                _empty_figure("Winter maximums will appear here."),
            )

        summary_records = analysis_data.get("summary", [])
        daily_records = analysis_data.get("daily", [])
        missing_records = analysis_data.get("missing", [])
        return (
            summary_records,
            _summary_columns(unit),
            daily_records,
            _daily_columns(unit),
            missing_records,
            _depth_figure(daily_records, unit),
            _winter_bar_figure(summary_records, unit),
        )

    return app
