import dash
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objs as go
import pandas as pd

from dateutil.rrule import rrule, MINUTELY
from dateutil.relativedelta import relativedelta
from typing import List
from dash.dependencies import Input, Output

profanity = pd.read_csv("data/election_night_extract.csv")
state_calls = pd.read_csv("data/state_results.csv")
profanity.loc[:, "time_central"] = pd.to_datetime(
    profanity.time
).dt.tz_convert("America/Chicago")
profanity.loc[:, "subject"] = profanity.subject.map(lambda x: x.capitalize())

start_time = profanity.time_central.min()
end_time = profanity.time_central.max()
slider_marks = {
    (m - start_time).seconds
    // 60: {
        "label": m.strftime("%-I:%M %p"),
        "style": {
            "transform": "rotate(55deg)",
            "font-size": "8px",
            "margin-top": "1px",
        },
    }
    for m in rrule(freq=MINUTELY, dtstart=start_time, until=end_time)
    if ((m - start_time).seconds // 60) % 30 == 0
}

biden_pic_url = (
    "https://pbs.twimg.com/profile_images/"
    "464835807837044737/vO0cnKR1_400x400.jpeg"
)
trump_pic_url = (
    "https://pbs.twimg.com/profile_images/"
    "874276197357596672/kUuht00m_400x400.jpg"
)

external_stylesheets = [dbc.themes.BOOTSTRAP]
dash_app = dash.Dash(external_stylesheets=external_stylesheets)
dash_app.title = "Profanity Power Index"
# This is for gunicorn to hook into.
app = dash_app.server

sidebar = dbc.Card(
    [
        html.Br(),
        html.Br(),
        html.Br(),
        dbc.FormGroup(
            [
                dbc.Label("Timeline"),
                dcc.RangeSlider(
                    id="time-slider",
                    min=0,
                    max=(end_time - start_time).seconds // 60,
                    step=1,
                    value=[0, (end_time - start_time).seconds // 60],
                    marks=slider_marks,
                ),
            ]
        ),
        dbc.FormGroup(
            [
                dbc.Label("Profanity"),
                dcc.Dropdown(
                    id="profanity-dropdown",
                    options=[
                        {"label": p, "value": p}
                        for p in profanity.word.unique().tolist()
                    ],
                    value=profanity.word.unique().tolist(),
                    multi=True,
                ),
            ]
        ),
        dbc.FormGroup(
            [
                dbc.Label("Candidate"),
                dcc.Dropdown(
                    id="candidate-dropdown",
                    options=[
                        {"label": c, "value": c}
                        for c in profanity.subject.unique().tolist()
                    ],
                    value=profanity.subject.unique().tolist(),
                    multi=True,
                ),
            ]
        ),
        dbc.Label("Collected from the Twitter public timeline."),
    ],
    body=True,
)

dash_app.layout = dbc.Container(
    [
        html.H1("Profanity Power Index"),
        html.Hr(),
        dbc.Row(
            [
                dbc.Col(sidebar, md=4, lg=3, xl=3),
                dbc.Col(
                    [
                        dcc.Graph(
                            "time-series-line", style={"margin": "auto"}
                        ),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        html.Img(
                                            src=trump_pic_url,
                                            className="img-fluid h-25",
                                            style={
                                                "margin": "auto",
                                                "padding": "5px",
                                            },
                                        ),
                                        html.Img(
                                            src=biden_pic_url,
                                            className="img-fluid h-25",
                                            style={
                                                "margin": "auto",
                                                "padding": "5px",
                                            },
                                        ),
                                    ],
                                    md=3,
                                    lg=3,
                                    xl=3,
                                ),
                                dbc.Col(
                                    [
                                        dcc.Graph(
                                            "trump-total-number",
                                            style={"margin": "auto"},
                                            className="h-25",
                                        ),
                                        dcc.Graph(
                                            "biden-total-number",
                                            style={"margin": "auto"},
                                            className="h-25",
                                        ),
                                    ],
                                    md=3,
                                    lg=3,
                                    xl=3,
                                ),
                                dbc.Col(
                                    dcc.Graph(
                                        "profanity-breakdown-bar",
                                        style={"margin": "auto"},
                                        className="h-50",
                                    ),
                                    md=6,
                                    lg=6,
                                    xl=6,
                                ),
                            ]
                        ),
                    ],
                    md=8,
                    lg=9,
                    xl=9,
                ),
            ]
        ),
    ],
    fluid=True,
)


def process_profanity(
    time_slider: List[int],
    profanity_dropdown: List[str],
    candidate_dropdown: List[str],
) -> pd.DataFrame:
    query_start_date = start_time + relativedelta(  # noqa
        minutes=time_slider[0]
    )
    query_end_date = start_time + relativedelta(minutes=time_slider[1])  # noqa

    return (
        profanity.query("time_central>=@query_start_date")
        .query("time_central<=@query_end_date")
        .query("subject.isin(@candidate_dropdown)")
        .query("word.isin(@profanity_dropdown)")
    )


@dash_app.callback(
    Output("time-series-line", "figure"),
    [
        Input("time-slider", "value"),
        Input("profanity-dropdown", "value"),
        Input("candidate-dropdown", "value"),
    ],
)
def time_series_line(
    time_slider: List[int],
    profanity_dropdown: List[str],
    candidate_dropdown: List[str],
) -> go.Figure:
    filtered_frame = (
        process_profanity(time_slider, profanity_dropdown, candidate_dropdown)
        .groupby(["time_central", "subject"])
        .agg({"count": "sum"})
        .reset_index()
    )

    plot = px.line(
        data_frame=filtered_frame,
        x="time_central",
        y="count",
        color="subject",
        title="Profanity by Candidate",
        labels={"time_central": "Time", "count": "Profanity Count"},
    )
    plot.update_layout(legend={"title": None}, xaxis_tickformat="%-I:%M %p")
    return plot


@dash_app.callback(
    Output("trump-total-number", "figure"),
    [
        Input("time-slider", "value"),
        Input("profanity-dropdown", "value"),
        Input("candidate-dropdown", "value"),
    ],
)
def trump_total_number(
    time_slider: List[int],
    profanity_dropdown: List[str],
    candidate_dropdown: List[str],
) -> go.Figure:
    filtered_frame = (
        process_profanity(time_slider, profanity_dropdown, candidate_dropdown)
        .groupby(["time_central", "subject"])
        .agg({"count": "sum"})
        .reset_index()
    )

    trump_count = filtered_frame.query("subject=='Trump'")["count"].sum()

    indicator = go.Figure()
    indicator.add_trace(go.Indicator(value=trump_count, mode="number"))
    indicator.update_layout(margin={"l": 0, "r": 0, "b": 0, "t": 0, "pad": 1})

    return indicator


@dash_app.callback(
    Output("biden-total-number", "figure"),
    [
        Input("time-slider", "value"),
        Input("profanity-dropdown", "value"),
        Input("candidate-dropdown", "value"),
    ],
)
def biden_total_number(
    time_slider: List[int],
    profanity_dropdown: List[str],
    candidate_dropdown: List[str],
) -> go.Figure:
    filtered_frame = process_profanity(
        time_slider, profanity_dropdown, candidate_dropdown
    )

    biden_count = filtered_frame.query("subject=='Biden'")["count"].sum()

    indicator = go.Figure()
    indicator.add_trace(go.Indicator(value=biden_count, mode="number"))
    indicator.update_layout(margin={"l": 0, "r": 0, "b": 0, "t": 0, "pad": 1})

    return indicator


@dash_app.callback(
    Output("profanity-breakdown-bar", "figure"),
    [
        Input("time-slider", "value"),
        Input("profanity-dropdown", "value"),
        Input("candidate-dropdown", "value"),
    ],
)
def profanity_breakdown_bar(
    time_slider: List[int],
    profanity_dropdown: List[str],
    candidate_dropdown: List[str],
) -> go.Figure:
    filtered_frame = (
        process_profanity(time_slider, profanity_dropdown, candidate_dropdown)
        .groupby(["subject", "word"])
        .agg({"count": "sum"})
        .reset_index()
    )

    plot = px.bar(
        data_frame=filtered_frame,
        x="word",
        y="count",
        color="subject",
        barmode="group",
        title="Profanity Breakdown",
        labels={"count": "Profanity Count", "word": "Profanity"},
    )
    plot.update_layout(legend={"title": None})
    return plot


if __name__ == "__main__":
    dash_app.run_server()
