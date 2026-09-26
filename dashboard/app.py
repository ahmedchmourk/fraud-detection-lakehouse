"""
Streamlit monitoring & analytics UI for the fraud-detection lakehouse.

Panels
  * Stream health  : rolling 60s throughput from the streaming engine (bronze/stream_metrics)
  * Live alerts    : most recently ingested high-risk transactions (silver)
  * Gold analytics : hourly fraud metrics, merchant risk, account leaderboard (gold)
"""

from __future__ import annotations

import os

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "5"))
BRONZE = os.getenv("BRONZE_URI", "s3://bronze")
SILVER = os.getenv("SILVER_URI", "s3://silver")
GOLD = os.getenv("GOLD_URI", "s3://gold")

ACCENT = "#22d3ee"
FRAUD = "#f43f5e"
MUTED = "#64748b"
TIER_COLORS = {"CRITICAL": "#f43f5e", "HIGH": "#f59e0b", "MEDIUM": "#eab308", "LOW": "#22c55e"}

st.set_page_config(page_title="Fraud Lakehouse Monitor", page_icon="🛡️", layout="wide")


def storage_options() -> dict[str, str]:
    return {
        "AWS_ENDPOINT_URL": os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
        "AWS_ALLOW_HTTP": "true",
    }


def query_delta(uri: str, sql: str) -> pd.DataFrame:
    """Run `sql` against the Delta table at `uri`, exposed to DuckDB as `t`."""
    from deltalake import DeltaTable

    try:
        dataset = DeltaTable(uri, storage_options=storage_options()).to_pyarrow_dataset()
    except Exception:  # noqa: BLE001 - table not created yet
        return pd.DataFrame()
    con = duckdb.connect()
    con.register("t", dataset)
    return con.execute(sql).df()


def plot_layout(fig: go.Figure, height: int = 300) -> go.Figure:
    fig.update_layout(
        height=height,
        margin=dict(l=10, r=10, t=30, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        font=dict(size=12),
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(148,163,184,0.15)")
    return fig


# --------------------------------------------------------------------------- #
# Header
# --------------------------------------------------------------------------- #
st.markdown(
    """
    <style>
      .block-container {padding-top: 1.6rem;}
      div[data-testid="stMetric"] {background: rgba(148,163,184,0.08); border: 1px solid rgba(148,163,184,0.18);
        border-radius: 12px; padding: 14px 16px;}
    </style>
    """,
    unsafe_allow_html=True,
)
st.title("🛡️ Real-Time Fraud & Anomaly Detection")
st.caption(
    "Kaggle Credit Card Fraud dataset → Redpanda → Streaming Engine → MinIO Delta Lake "
    "(Bronze → Silver → Gold) · refreshed every "
    f"{REFRESH_SECONDS}s"
)


@st.fragment(run_every=REFRESH_SECONDS)
def live_panel() -> None:
    metrics = query_delta(
        f"{BRONZE}/stream_metrics",
        "SELECT * FROM t WHERE window_end >= now() - INTERVAL 30 MINUTES ORDER BY window_end",
    )
    kpis = query_delta(
        f"{SILVER}/cleansed_transactions",
        """SELECT count(*) AS txns, sum(is_fraud) AS fraud, count(*) FILTER (WHERE is_flagged) AS flagged,
                  count(*) FILTER (WHERE is_flagged AND is_fraud = 1) AS tp, sum(amount) AS amount
           FROM t""",
    )

    latest = metrics.iloc[-1] if not metrics.empty else None
    k = kpis.iloc[0] if not kpis.empty else None
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Throughput (60s)", f"{latest.events_per_second:.1f} msg/s" if latest is not None else "—")
    c2.metric("Events (engine session)", f"{int(latest.total_events):,}" if latest is not None else "—")
    c3.metric("Silver transactions", f"{int(k.txns):,}" if k is not None and k.txns else "—")
    c4.metric(
        "Flagged / confirmed fraud",
        f"{int(k.flagged or 0):,} / {int(k.fraud or 0):,}" if k is not None and k.txns else "—",
    )
    precision = (k.tp / k.flagged) if k is not None and k.flagged else None
    c5.metric("Model precision", f"{precision:.1%}" if precision is not None else "—")

    left, right = st.columns([3, 2])
    with left:
        st.subheader("Stream ingestion throughput")
        if metrics.empty:
            st.info("Waiting for the streaming engine to emit window metrics…")
        else:
            fig = go.Figure()
            fig.add_scatter(
                x=metrics.window_end,
                y=metrics.events_per_second,
                name="events/s (60s window)",
                line=dict(color=ACCENT, width=2),
                fill="tozeroy",
                fillcolor="rgba(34,211,238,0.12)",
            )
            fig.add_bar(
                x=metrics.window_end,
                y=metrics.fraud_in_window,
                name="fraud in window",
                marker_color=FRAUD,
                yaxis="y2",
                opacity=0.8,
            )
            fig.update_layout(yaxis2=dict(overlaying="y", side="right", showgrid=False, title="fraud"))
            st.plotly_chart(plot_layout(fig), width="stretch")

    with right:
        st.subheader("🚨 Live fraud alert feed")
        alerts = query_delta(
            f"{SILVER}/cleansed_transactions",
            """SELECT strftime(ingested_at, '%H:%M:%S') AS ingested, risk_tier AS tier, risk_score AS score,
                      amount, account_id, merchant_category AS category, location,
                      CASE WHEN is_fraud = 1 THEN '✅ fraud' ELSE '— legit' END AS label
               FROM t WHERE is_flagged ORDER BY ingested_at DESC LIMIT 12""",
        )
        if alerts.empty:
            st.info("No high-risk transactions yet.")
        else:
            st.dataframe(
                alerts,
                hide_index=True,
                width="stretch",
                height=320,
                column_config={
                    "score": st.column_config.ProgressColumn("score", min_value=0, max_value=100, format="%.0f"),
                    "amount": st.column_config.NumberColumn("amount", format="$%.2f"),
                },
            )


@st.fragment(run_every=REFRESH_SECONDS * 3)
def gold_panel() -> None:
    st.divider()
    st.header("Gold layer analytics")

    hourly = query_delta(f"{GOLD}/hourly_fraud_rate_metrics", "SELECT * FROM t ORDER BY event_hour")
    merchants = query_delta(
        f"{GOLD}/daily_merchant_risk_summary",
        """SELECT merchant_category, sum(txn_count) AS txns, sum(confirmed_fraud_count) AS fraud,
                  sum(flagged_txn_count) AS flagged, round(avg(avg_risk_score), 3) AS avg_risk
           FROM t GROUP BY merchant_category ORDER BY flagged DESC""",
    )
    leaderboard = query_delta(
        f"{GOLD}/high_risk_account_leaderboard",
        """SELECT risk_rank, account_id, txn_count, total_amount, flagged_txn_count, confirmed_fraud_count,
                  max_risk_score, avg_risk_score, distinct_locations FROM t ORDER BY risk_rank LIMIT 15""",
    )

    a, b = st.columns(2)
    with a:
        st.subheader("Hourly fraud rate (event time)")
        if hourly.empty:
            st.info("Gold tables are built by the transformer every 30s.")
        else:
            fig = go.Figure()
            fig.add_bar(x=hourly.event_hour, y=hourly.txn_count, name="transactions", marker_color=MUTED, opacity=0.5)
            fig.add_scatter(
                x=hourly.event_hour,
                y=hourly.fraud_rate_pct,
                name="fraud rate %",
                yaxis="y2",
                line=dict(color=FRAUD, width=2),
                mode="lines+markers",
            )
            fig.update_layout(yaxis2=dict(overlaying="y", side="right", showgrid=False, title="fraud %"))
            st.plotly_chart(plot_layout(fig), width="stretch")
    with b:
        st.subheader("Detection quality by hour")
        if not hourly.empty:
            q = hourly.melt(id_vars="event_hour", value_vars=["true_positives", "false_positives", "false_negatives"])
            fig = px.bar(
                q,
                x="event_hour",
                y="value",
                color="variable",
                barmode="stack",
                color_discrete_map={
                    "true_positives": "#22c55e",
                    "false_positives": "#f59e0b",
                    "false_negatives": FRAUD,
                },
            )
            st.plotly_chart(plot_layout(fig), width="stretch")

    c, d = st.columns([2, 3])
    with c:
        st.subheader("Merchant category risk")
        if not merchants.empty:
            fig = px.bar(
                merchants,
                x="flagged",
                y="merchant_category",
                orientation="h",
                color="avg_risk",
                color_continuous_scale=["#1e3a5f", ACCENT, FRAUD],
            )
            fig.update_yaxes(categoryorder="total ascending")
            st.plotly_chart(plot_layout(fig, 340), width="stretch")
    with d:
        st.subheader("High-risk account leaderboard")
        if not leaderboard.empty:
            st.dataframe(
                leaderboard,
                hide_index=True,
                width="stretch",
                height=340,
                column_config={
                    "max_risk_score": st.column_config.ProgressColumn(
                        "max risk", min_value=0, max_value=100, format="%.0f"
                    ),
                    "total_amount": st.column_config.NumberColumn("total $", format="$%.2f"),
                },
            )


live_panel()
gold_panel()
