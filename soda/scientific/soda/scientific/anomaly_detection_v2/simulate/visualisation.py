import pandas as pd
import plotly.graph_objects as go

from soda.scientific.anomaly_detection_v2.simulate.pydantic_models import (
    AnomalyDetectionResults,
)


def visualize_results(results: AnomalyDetectionResults) -> go.Figure:
    df = pd.DataFrame([result.model_dump() for result in results.results])
    df = df.sort_values(by="ds")
    df = df.drop("label", axis=1)
    df = df.dropna().reset_index(drop=True)
    # Cast bounds to float
    df.fail_upper_bound = df.fail_upper_bound.astype(float)
    df.fail_lower_bound = df.fail_lower_bound.astype(float)
    df.warn_upper_bound = df.warn_upper_bound.astype(float)
    df.warn_lower_bound = df.warn_lower_bound.astype(float)
    df.value = df.value.astype(float)

    # Create plotyly figure
    fig = go.Figure()

    # Add y and y^ traces
    fig.add_trace(go.Scatter(x=df.ds, y=df.value, mode="lines", name="y"))
    fig.add_trace(go.Scatter(x=df.ds, y=df.yhat, mode="lines", name="y^", line=dict(dash="dash")))

    # Fill the area between bounds
    fig.add_trace(
        go.Scatter(
            x=df.ds.tolist() + df.ds.tolist()[::-1],
            y=df.fail_upper_bound.tolist() + df.fail_lower_bound.tolist()[::-1],
            fill="toself",
            fillcolor="rgba(255,255,0,0.2)",  # light yellow for fail
            line=dict(color="rgba(255,255,0,0.2)"),
            name="warning range",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=df.ds.tolist() + df.ds.tolist()[::-1],
            y=df.warn_upper_bound.tolist() + df.warn_lower_bound.tolist()[::-1],
            fill="toself",
            fillcolor="rgba(0,255,0,0.2)",  # light green for pass
            line=dict(color="rgba(255,255,255,0)"),
            name="pass range",
        )
    )

    # Mark data points that are outside the bounds
    fail_points = df[(df.value > df.fail_upper_bound) | (df.value < df.fail_lower_bound)]
    warn_points = df[
        ((df.value > df.warn_upper_bound) & (df.value <= df.fail_upper_bound))
        | ((df.value < df.warn_lower_bound) & (df.value >= df.fail_lower_bound))
    ]
    pass_points = df[(df.value <= df.warn_upper_bound) & (df.value >= df.warn_lower_bound)]
    custom_data = ["yhat", "warn_lower_bound", "fail_lower_bound", "warn_upper_bound", "fail_upper_bound"]
    # Hover over template
    hover_over_template = (
        "Scan Time: %{x}<br>"
        + "Measurement: %{y}<br>"  # Display X value
        + "y^: %{customdata[0]:.4f}<br>"  # Display Y value
        + "Warn Upper Bound: %{customdata[3]:.4f}<br>"  # Display y^ value
        + "Fail Upper Bound: %{customdata[4]:.4f}<br>"  # Display warn_upper_bound value
        + "Warn Lower Bound: %{customdata[1]:.4f}<br>"  # Display fail_upper_bound value
        + "Fail Lower Bound: %{customdata[2]:.4f}<br>"  # Display warn_lower_bound value
        + "<extra></extra>"  # Display fail_lower_bound value
    )
    fig.add_trace(
        go.Scatter(
            x=fail_points.ds,
            y=fail_points.value,
            mode="markers",
            name="fail",
            marker=dict(color="red", size=10),
            hovertemplate=hover_over_template,
            customdata=fail_points[custom_data],
        )
    )
    fig.add_trace(
        go.Scatter(
            x=warn_points.ds,
            y=warn_points.value,
            mode="markers",
            name="warn",
            marker=dict(color="yellow", size=10),
            hovertemplate=hover_over_template,
            customdata=warn_points[custom_data],
        )
    )
    fig.add_trace(
        go.Scatter(
            x=pass_points.ds,
            y=pass_points.value,
            mode="markers",
            name="pass",
            marker=dict(color="green", size=3),
            hovertemplate=hover_over_template,
            customdata=pass_points[custom_data],
        )
    )

    fig.update_layout(autosize=True, margin=dict(l=0, r=0, b=0, t=0, pad=0))
    return fig
