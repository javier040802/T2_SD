from __future__ import annotations

from pathlib import Path
import json
import os

import matplotlib.pyplot as plt
import pandas as pd
import requests
import streamlit as st


st.set_page_config(page_title="SD T2 Dashboard", layout="wide")
st.title("Dashboard de métricas - Tarea 2 Sistemas Distribuidos")
st.caption("Monitorea throughput, latencia, reintentos, DLQ y backlog estimado.")

METRICS_URL = os.getenv("METRICS_SERVICE_URL", "http://metrics-service:8002")
metrics_path = Path("/data/metrics.jsonl")


@st.cache_data(ttl=3)
def fetch_summary() -> dict:
    try:
        r = requests.get(f"{METRICS_URL}/summary", timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception:
        if metrics_path.exists():
            rows = []
            with metrics_path.open("r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        rows.append(json.loads(line))
            if rows:
                df = pd.DataFrame(rows)
                success = df[df["event_type"].isin(["processed", "recovered"])]
                if not success.empty:
                    return {
                        "generated": int(df.loc[df["event_type"] == "generated", "request_id"].nunique()),
                        "processed": int(success["request_id"].nunique()),
                        "recovered": int(df.loc[df["event_type"] == "recovered", "request_id"].nunique()),
                        "retries": int(df.loc[df["event_type"] == "retry", "request_id"].count()),
                        "dlq": int(df.loc[df["event_type"] == "dlq", "request_id"].nunique()),
                        "latency_p50_ms": float(success["latency_ms"].astype(float).quantile(0.5)),
                        "latency_p95_ms": float(success["latency_ms"].astype(float).quantile(0.95)),
                        "throughput_qps": float(success.shape[0] / max(1.0, df["timestamp"].max() - df["timestamp"].min())),
                        "backlog_size": int(df.get("backlog_size", pd.Series([0])).max()),
                        "recovery_time_ms": 0.0,
                        "retry_rate": float(df.loc[df["event_type"] == "retry"].shape[0] / max(1, df.loc[df["event_type"] == "generated"].shape[0])),
                        "recovery_rate": 0.0,
                        "dlq_rate": 0.0,
                        "hit_rate": float(success["cache_hit"].fillna(False).mean()) if "cache_hit" in success else 0.0,
                    }
        return {"generated": 0}


@st.cache_data(ttl=3)
def fetch_events(limit: int = 200) -> pd.DataFrame:
    try:
        r = requests.get(f"{METRICS_URL}/events?limit={limit}", timeout=5)
        r.raise_for_status()
        payload = r.json().get("events", [])
        return pd.DataFrame(payload)
    except Exception:
        if metrics_path.exists():
            rows = []
            with metrics_path.open("r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        rows.append(json.loads(line))
            return pd.DataFrame(rows[-limit:])
        return pd.DataFrame()


summary = fetch_summary()
df = fetch_events()

try:
    from streamlit_autorefresh import st_autorefresh
    st_autorefresh(interval=3000, key="dashboard_refresh")
except Exception:
    pass


if summary.get("generated", 0) == 0 and summary.get("count", 0) == 0:
    st.info("Aún no hay tráfico registrado.")
else:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Procesadas", f"{summary.get('processed', summary.get('count', 0))}")
    c2.metric("Throughput", f"{summary.get('throughput_qps', 0.0):.2f} req/s")
    c3.metric("P95 latency", f"{summary.get('latency_p95_ms', 0.0):.2f} ms")
    c4.metric("Backlog", f"{summary.get('backlog_size', 0)}")

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Retry rate", f"{summary.get('retry_rate', 0.0):.2%}")
    c6.metric("Recovery rate", f"{summary.get('recovery_rate', 0.0):.2%}")
    c7.metric("DLQ rate", f"{summary.get('dlq_rate', 0.0):.2%}")
    c8.metric("Failures", f"{summary.get('failures', 0)}")

    c9, c10 = st.columns(2)
    with c9:
        st.subheader("Evolución de backlog")
        if not df.empty and "backlog_size" in df:
            fig, ax = plt.subplots()
            ax.plot(df.index, df["backlog_size"].fillna(0).astype(float).cummax())
            ax.set_xlabel("evento")
            ax.set_ylabel("backlog")
            st.pyplot(fig, clear_figure=True)
        else:
            st.caption("Sin datos para mostrar.")

    with c10:
        st.subheader("Latencia por tipo de evento")
        if not df.empty and "event_type" in df:
            filtered = df[df["event_type"].isin(["processed", "recovered"])]
            if not filtered.empty:
                order = sorted(filtered["qtype"].dropna().unique())
                data = [filtered[filtered["qtype"] == q]["latency_ms"] for q in order]
                fig, ax = plt.subplots()
                ax.boxplot(data, labels=order)
                ax.set_ylabel("ms")
                st.pyplot(fig, clear_figure=True)
            else:
                st.caption("Sin datos de procesamiento.")
        else:
            st.caption("Sin datos para mostrar.")

    st.subheader("Eventos recientes")
    if not df.empty:
        st.dataframe(df.tail(15), use_container_width=True)
    else:
        st.caption("Sin eventos.")
