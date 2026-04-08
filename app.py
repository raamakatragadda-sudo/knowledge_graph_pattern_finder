"""
app.py -- Streamlit dashboard for P2P Knowledge Graph Pattern Analysis.

Three-step workflow:
  1. POPULATE  -- Generate synthetic P2P cases with user-controlled deviations
  2. EXPLORE   -- Visualise the knowledge graph (cases, events, pipeline)
  3. ANALYSE   -- Run the correlation finder and compare against ground truth
"""

from __future__ import annotations

import json
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import networkx as nx
import pandas as pd

# ---------------------------------------------------------------------------
# Make sure project root is importable
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).resolve().parent))

from query import load_env_auto, neo4j_driver, extract_cases
from scenario_multiple import (
    _STAGE_SEQ,
    _STUCK_WEIGHTS,
    _DELAY_PROB,
    _rng,
    build_case_payload,
    create_constraints,
    upsert_open_case_batch,
    chunked,
)
from full_general_correlation_finder import (
    discover_pipeline,
    compute_baselines_and_flag,
    tokenise_cases,
    tokenise_with_field,
    mine_patterns,
    filter_correlation_patterns,
    mine_field_patterns,
    MIN_SUPPORT_FRAC,
    SLOW_MULTIPLIER,
)


# ===========================================================================
# PAGE CONFIG
# ===========================================================================

st.set_page_config(
    page_title="P2P Pattern Finder",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ===========================================================================
# HELPERS
# ===========================================================================

def _compute_ground_truth(
    num_cases: int,
    delay_prob: float,
    stuck_weights: List[int],
) -> pd.DataFrame:
    """
    Replay the RNG from scenario_multiple to know exactly which cases
    have which delays and stuck points. This is the answer key.
    """
    rows = []
    for idx in range(1, num_cases + 1):
        rng = _rng(idx)
        stuck_stage = rng.choices(_STAGE_SEQ, weights=stuck_weights, k=1)[0]
        stuck_idx = _STAGE_SEQ.index(stuck_stage)
        delayed_stages = set()
        for stage in _STAGE_SEQ[:stuck_idx]:
            if rng.random() < delay_prob:
                delayed_stages.add(stage)
        rows.append({
            "case_id": f"CASE_{idx}",
            "stuck_stage": stuck_stage,
            "delayed_stages": sorted(delayed_stages),
            "num_delays": len(delayed_stages),
            "company_code": f"COMP_{((idx - 1) % 2) + 1}",
            "supplier_id": f"SUP_{((idx - 1) % 7) + 1}",
            "plant_id": f"PLANT_{((idx - 1) % 5) + 1}",
            "material_id": f"MAT_{((idx - 1) % 10) + 1}",
        })
    return pd.DataFrame(rows)


def _build_pipeline_graph(
    canonical_order: List[str],
    transitions: List[Tuple[str, str, str]],
    stuck_counts: Dict[str, int],
    slow_counts: Dict[str, int],
) -> go.Figure:
    """Build a Plotly Sankey-style node graph of the discovered pipeline."""
    G = nx.DiGraph()
    for ev in canonical_order:
        G.add_node(ev)
    for _, ev_from, ev_to in transitions:
        G.add_edge(ev_from, ev_to)

    pos = {}
    n = len(canonical_order)
    for i, ev in enumerate(canonical_order):
        pos[ev] = (i / max(n - 1, 1), 0.5)

    edge_x, edge_y = [], []
    for u, v in G.edges():
        x0, y0 = pos[u]
        x1, y1 = pos[v]
        edge_x += [x0, x1, None]
        edge_y += [y0, y1, None]

    node_x = [pos[n][0] for n in canonical_order]
    node_y = [pos[n][1] for n in canonical_order]
    node_text = []
    node_size = []
    node_color = []

    for ev in canonical_order:
        sl = slow_counts.get(ev, 0)
        st_c = stuck_counts.get(ev, 0)
        node_text.append(f"{ev}<br>SLOW: {sl}<br>STUCK: {st_c}")
        node_size.append(20 + st_c * 2 + sl)
        if st_c > 0 and sl > 0:
            node_color.append("#e74c3c")
        elif st_c > 0:
            node_color.append("#f39c12")
        elif sl > 0:
            node_color.append("#e67e22")
        else:
            node_color.append("#2ecc71")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=edge_x, y=edge_y, mode="lines",
        line=dict(width=2, color="#95a5a6"),
        hoverinfo="none",
    ))
    fig.add_trace(go.Scatter(
        x=node_x, y=node_y, mode="markers+text",
        marker=dict(size=node_size, color=node_color, line=dict(width=2, color="#2c3e50")),
        text=[ev.replace("_", "\n") for ev in canonical_order],
        textposition="top center",
        hovertext=node_text,
        hoverinfo="text",
        textfont=dict(size=10),
    ))
    fig.update_layout(
        showlegend=False,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        margin=dict(l=20, r=20, t=40, b=20),
        height=250,
        title="Discovered Pipeline (node size = delay severity, red = stuck + slow)",
    )
    return fig


def _build_case_timeline_fig(events: List[Dict], case_id: str) -> go.Figure:
    """Build a horizontal timeline for a single case."""
    if not events:
        return go.Figure()
    sorted_evts = sorted(events, key=lambda e: e["event_time"] or "")
    etypes = [ev["event_type"] for ev in sorted_evts]
    times = [ev["event_time"] for ev in sorted_evts]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=times, y=[1] * len(times),
        mode="markers+text",
        marker=dict(size=14, color=px.colors.qualitative.Set2[:len(times)]),
        text=etypes,
        textposition="top center",
        textfont=dict(size=9),
        hovertext=[f"{e}<br>{t}" for e, t in zip(etypes, times)],
        hoverinfo="text",
    ))
    fig.update_layout(
        title=f"Timeline: {case_id}",
        yaxis=dict(visible=False),
        xaxis=dict(title="Time"),
        height=200,
        margin=dict(l=20, r=20, t=40, b=40),
    )
    return fig


# ===========================================================================
# SIDEBAR
# ===========================================================================

st.sidebar.title("P2P Pattern Finder")
step = st.sidebar.radio(
    "Step",
    ["1. Populate KG", "2. Explore KG", "3. Analyse & Accuracy"],
    index=0,
)

# ===========================================================================
# STEP 1 -- POPULATE
# ===========================================================================

if step == "1. Populate KG":
    st.header("Step 1: Populate the Knowledge Graph")
    st.markdown(
        "Configure the synthetic data parameters below, then click **Generate** "
        "to create cases in Neo4j. Each case follows the P2P pipeline and can "
        "have random delays injected at each stage."
    )

    col1, col2 = st.columns(2)
    with col1:
        num_cases = st.slider("Number of cases", 10, 500, 100, step=10)
        delay_prob = st.slider(
            "Delay probability (per stage)",
            0.0, 1.0, _DELAY_PROB, step=0.05,
            help="Chance that each stage before the stuck point is abnormally slow.",
        )
    with col2:
        st.markdown("**Stuck-stage weights** (higher = more cases get stuck here)")
        w_labels = ["PR_SUBMITTED", "PO_APPROVED", "PO_ISSUED", "GOODS_RECEIVED", "INVOICE_RECEIVED", "INVOICE_VALIDATED"]
        stuck_weights = []
        cols_w = st.columns(3)
        for i, label in enumerate(w_labels):
            with cols_w[i % 3]:
                w = st.number_input(label, min_value=0, max_value=100, value=_STUCK_WEIGHTS[i], key=f"w_{i}")
                stuck_weights.append(w)

    # Preview ground truth
    gt_df = _compute_ground_truth(num_cases, delay_prob, stuck_weights)
    st.session_state["gt_df"] = gt_df
    st.session_state["num_cases"] = num_cases
    st.session_state["delay_prob"] = delay_prob
    st.session_state["stuck_weights"] = stuck_weights

    st.subheader("Preview: Ground Truth Distribution")
    col_a, col_b = st.columns(2)
    with col_a:
        stuck_dist = gt_df["stuck_stage"].value_counts().reindex(w_labels, fill_value=0)
        fig_stuck = px.bar(
            x=stuck_dist.index, y=stuck_dist.values,
            labels={"x": "Stuck Stage", "y": "Count"},
            title="Where Cases Get Stuck",
            color=stuck_dist.values,
            color_continuous_scale="Reds",
        )
        fig_stuck.update_layout(coloraxis_showscale=False, height=350)
        st.plotly_chart(fig_stuck, use_container_width=True)

    with col_b:
        delay_counts = Counter()
        for _, row in gt_df.iterrows():
            for d in row["delayed_stages"]:
                delay_counts[d] += 1
        delay_ser = pd.Series(delay_counts).reindex(w_labels, fill_value=0)
        fig_delay = px.bar(
            x=delay_ser.index, y=delay_ser.values,
            labels={"x": "Stage", "y": "Slow Count"},
            title="Where Delays Are Injected (SLOW transitions)",
            color=delay_ser.values,
            color_continuous_scale="Oranges",
        )
        fig_delay.update_layout(coloraxis_showscale=False, height=350)
        st.plotly_chart(fig_delay, use_container_width=True)

    # Co-occurrence heatmap
    co_matrix = pd.DataFrame(0, index=w_labels, columns=w_labels)
    for _, row in gt_df.iterrows():
        for d in row["delayed_stages"]:
            co_matrix.loc[d, row["stuck_stage"]] += 1

    fig_heat = px.imshow(
        co_matrix.values,
        x=co_matrix.columns, y=co_matrix.index,
        labels=dict(x="Stuck At", y="Slow At", color="Count"),
        title="Ground Truth: SLOW -> STUCK Co-occurrence",
        color_continuous_scale="YlOrRd",
        text_auto=True,
    )
    fig_heat.update_layout(height=400)
    st.plotly_chart(fig_heat, use_container_width=True)

    # Generate button
    if st.button("Generate & Load into Neo4j", type="primary"):
        with st.spinner("Loading cases into Neo4j..."):
            # Monkey-patch the module-level variables for this run
            import scenario_multiple as sm
            sm._DELAY_PROB = delay_prob
            sm._STUCK_WEIGHTS = stuck_weights

            load_env_auto()
            driver = neo4j_driver()
            with driver.session() as session:
                session.execute_write(create_constraints)
                all_ids = list(range(1, num_cases + 1))
                for batch_ids in chunked(all_ids, 100):
                    payloads = [build_case_payload(idx) for idx in batch_ids]
                    session.execute_write(upsert_open_case_batch, payloads)
            driver.close()

        st.success(f"Successfully loaded {num_cases} cases into Neo4j!")
        st.session_state["kg_loaded"] = True


# ===========================================================================
# STEP 2 -- EXPLORE
# ===========================================================================

elif step == "2. Explore KG":
    st.header("Step 2: Explore the Knowledge Graph")

    with st.spinner("Querying Neo4j..."):
        load_env_auto()
        driver = neo4j_driver()
        cases = extract_cases(driver)
        driver.close()

    if not cases:
        st.warning("No cases found in Neo4j. Run Step 1 first.")
        st.stop()

    st.session_state["cases"] = cases
    st.success(f"Loaded {len(cases)} cases from Neo4j.")

    # Overview metrics
    open_count = sum(1 for c in cases if c["case_status"] == "OPEN")
    completed_count = len(cases) - open_count
    avg_events = sum(len(c.get("events") or []) for c in cases) / len(cases) if cases else 0

    m1, m2, m3 = st.columns(3)
    m1.metric("Total Cases", len(cases))
    m2.metric("Open Cases", open_count)
    m3.metric("Avg Events/Case", f"{avg_events:.1f}")

    # Event type distribution
    event_type_counts = Counter()
    for case in cases:
        for ev in (case.get("events") or []):
            if ev.get("event_type"):
                event_type_counts[ev["event_type"]] += 1

    fig_events = px.bar(
        x=list(event_type_counts.keys()),
        y=list(event_type_counts.values()),
        labels={"x": "Event Type", "y": "Count"},
        title="Event Type Distribution Across All Cases",
        color=list(event_type_counts.values()),
        color_continuous_scale="Viridis",
    )
    fig_events.update_layout(coloraxis_showscale=False, height=350)
    st.plotly_chart(fig_events, use_container_width=True)

    # Last event distribution (where cases are stuck)
    last_event_counts = Counter()
    for case in cases:
        events = case.get("events") or []
        if events:
            sorted_evts = sorted(events, key=lambda e: e["event_time"] or "")
            last_event_counts[sorted_evts[-1]["event_type"]] += 1

    fig_last = px.pie(
        names=list(last_event_counts.keys()),
        values=list(last_event_counts.values()),
        title="Cases by Last Event (where they are stuck)",
    )
    fig_last.update_layout(height=400)
    st.plotly_chart(fig_last, use_container_width=True)

    # Case explorer
    st.subheader("Case Timeline Explorer")
    case_ids = [c["case_id"] for c in cases]
    selected = st.selectbox("Select a case", case_ids)
    sel_case = next(c for c in cases if c["case_id"] == selected)

    root_attrs = sel_case.get("root_attributes") or "{}"
    if isinstance(root_attrs, str):
        root_attrs = json.loads(root_attrs)

    col_info, col_attrs = st.columns(2)
    with col_info:
        st.markdown(f"**Status:** {sel_case['case_status']}")
        st.markdown(f"**Last Event:** {sel_case.get('last_event_time', 'N/A')}")
        st.markdown(f"**Events:** {len(sel_case.get('events') or [])}")
    with col_attrs:
        st.json(root_attrs)

    fig_timeline = _build_case_timeline_fig(sel_case.get("events") or [], selected)
    st.plotly_chart(fig_timeline, use_container_width=True)

    # Events table
    if sel_case.get("events"):
        evt_df = pd.DataFrame(sel_case["events"])
        st.dataframe(evt_df, use_container_width=True, hide_index=True)


# ===========================================================================
# STEP 3 -- ANALYSE & ACCURACY
# ===========================================================================

elif step == "3. Analyse & Accuracy":
    st.header("Step 3: Run Correlation Finder & Check Accuracy")

    # Load cases
    with st.spinner("Extracting cases from Neo4j..."):
        load_env_auto()
        driver = neo4j_driver()
        cases = extract_cases(driver)
        driver.close()

    if not cases:
        st.warning("No cases in Neo4j. Run Step 1 first.")
        st.stop()

    total_cases = len(cases)
    st.info(f"Loaded {total_cases} cases. Running correlation finder...")

    # Run the full pipeline
    with st.spinner("Discovering pipeline..."):
        canonical_order, transitions, blocking_point_map, drill_fields = discover_pipeline(cases)

    with st.spinner("Computing baselines..."):
        baselines, annotated = compute_baselines_and_flag(
            cases, transitions, blocking_point_map, drill_fields
        )

    with st.spinner("Mining patterns..."):
        sequences, seq_cases = tokenise_cases(annotated, transitions)
        min_support = max(2, int(len(sequences) * MIN_SUPPORT_FRAC))
        mined = mine_patterns(sequences, min_support)
        stage_results = filter_correlation_patterns(mined, sequences, seq_cases, total_cases)
        field_results = mine_field_patterns(annotated, transitions, drill_fields, min_support, total_cases)

    # ---- PIPELINE VISUALISATION ----
    st.subheader("Discovered Pipeline")

    slow_counter = Counter()
    stuck_counter = Counter()
    for ac in annotated:
        for s in ac["slow_steps"]:
            slow_counter[s] += 1
        if ac["blocking_point"]:
            stuck_counter[ac["blocking_point"]] += 1

    fig_pipeline = _build_pipeline_graph(canonical_order, transitions, stuck_counter, slow_counter)
    st.plotly_chart(fig_pipeline, use_container_width=True)

    # Baselines table
    st.subheader("Transition Baselines")
    bl_rows = []
    for step_name, _, _ in transitions:
        med = baselines.get(step_name, 0)
        bl_rows.append({
            "Transition": step_name,
            "Median (hrs)": round(med / 60, 2),
            "Threshold (hrs)": round(med * SLOW_MULTIPLIER / 60, 2),
            "SLOW count": slow_counter.get(step_name, 0),
            "STUCK count": stuck_counter.get(step_name, 0),
        })
    st.dataframe(pd.DataFrame(bl_rows), use_container_width=True, hide_index=True)

    # Delay distribution charts
    col_sl, col_stk = st.columns(2)
    with col_sl:
        fig_sl = px.bar(
            x=list(slow_counter.keys()), y=list(slow_counter.values()),
            title="SLOW Transitions Detected", labels={"x": "Step", "y": "Count"},
            color=list(slow_counter.values()), color_continuous_scale="Oranges",
        )
        fig_sl.update_layout(coloraxis_showscale=False, height=300)
        st.plotly_chart(fig_sl, use_container_width=True)
    with col_stk:
        fig_stk = px.bar(
            x=list(stuck_counter.keys()), y=list(stuck_counter.values()),
            title="STUCK Points Detected", labels={"x": "Step", "y": "Count"},
            color=list(stuck_counter.values()), color_continuous_scale="Reds",
        )
        fig_stk.update_layout(coloraxis_showscale=False, height=300)
        st.plotly_chart(fig_stk, use_container_width=True)

    # ---- CORRELATION PATTERNS ----
    st.subheader("Correlation Patterns Found")

    if stage_results:
        pat_rows = []
        for r in stage_results:
            pat_rows.append({
                "Pattern": r["pattern_str"],
                "Cause": ", ".join(r["cause"]),
                "Effect": ", ".join(r["effect"]),
                "Support": r["support_count"],
                "Support %": f"{r['support_frac']:.1%}",
                "Avg Stall (days)": r["avg_stall_days"],
                "Rank Score": r["rank_score"],
            })
        st.dataframe(pd.DataFrame(pat_rows), use_container_width=True, hide_index=True)
    else:
        st.warning("No correlation patterns found.")

    if field_results:
        st.subheader("Field-Enriched Patterns (Top 15)")
        fp_rows = []
        for r in field_results[:15]:
            fp_rows.append({
                "Field": r.get("enrichment_field", ""),
                "Pattern": r["pattern_str"],
                "Support": r["support_count"],
                "Support %": f"{r['support_frac']:.1%}",
                "Avg Stall (days)": r["avg_stall_days"],
            })
        st.dataframe(pd.DataFrame(fp_rows), use_container_width=True, hide_index=True)

    # ================================================================
    # ACCURACY COMPARISON
    # ================================================================
    st.markdown("---")
    st.header("Accuracy: Finder vs Ground Truth")

    gt_df = st.session_state.get("gt_df")
    num_gt = st.session_state.get("num_cases")

    if gt_df is None or num_gt != total_cases:
        # Recompute with defaults if session was lost
        gt_df = _compute_ground_truth(total_cases, _DELAY_PROB, _STUCK_WEIGHTS)

    # --- SLOW detection accuracy ---
    st.subheader("SLOW Detection Accuracy (per case)")

    gt_slow_per_case = {row["case_id"]: set(row["delayed_stages"]) for _, row in gt_df.iterrows()}
    det_slow_per_case = {ac["case_id"]: set(ac["slow_steps"]) for ac in annotated}

    case_rows = []
    tp_total = fp_total = fn_total = 0
    for cid in sorted(gt_slow_per_case.keys()):
        gt_set = gt_slow_per_case.get(cid, set())
        det_set = det_slow_per_case.get(cid, set())
        tp = gt_set & det_set
        fp = det_set - gt_set
        fn = gt_set - det_set
        tp_total += len(tp)
        fp_total += len(fp)
        fn_total += len(fn)
        if gt_set or det_set:
            case_rows.append({
                "Case": cid,
                "Truth SLOW": ", ".join(sorted(gt_set)) or "-",
                "Detected SLOW": ", ".join(sorted(det_set)) or "-",
                "TP": len(tp),
                "FP": len(fp),
                "FN": len(fn),
                "Match": "EXACT" if tp == gt_set and not fp else "PARTIAL" if tp else "MISS",
            })

    precision = tp_total / (tp_total + fp_total) if (tp_total + fp_total) > 0 else 0
    recall = tp_total / (tp_total + fn_total) if (tp_total + fn_total) > 0 else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

    col_p, col_r, col_f = st.columns(3)
    col_p.metric("Precision", f"{precision:.1%}")
    col_r.metric("Recall", f"{recall:.1%}")
    col_f.metric("F1 Score", f"{f1:.1%}")

    with st.expander(f"Per-case SLOW detection detail ({len(case_rows)} cases with delays)"):
        st.dataframe(pd.DataFrame(case_rows), use_container_width=True, hide_index=True)

    # --- STUCK detection accuracy ---
    st.subheader("STUCK Detection Accuracy")

    gt_stuck_per_case = {row["case_id"]: row["stuck_stage"] for _, row in gt_df.iterrows()}
    det_stuck_per_case = {ac["case_id"]: ac["blocking_point"] for ac in annotated}

    stuck_tp = stuck_total = 0
    stuck_detail = []
    for cid in sorted(gt_stuck_per_case.keys()):
        gt_s = gt_stuck_per_case.get(cid)
        det_s = det_stuck_per_case.get(cid)
        stuck_total += 1
        match = gt_s == det_s
        if match:
            stuck_tp += 1
        stuck_detail.append({
            "Case": cid,
            "Truth Stuck": gt_s,
            "Detected Stuck": det_s or "(none)",
            "Match": "YES" if match else "NO",
        })

    stuck_acc = stuck_tp / stuck_total if stuck_total > 0 else 0
    st.metric("STUCK Accuracy", f"{stuck_acc:.1%}", f"{stuck_tp}/{stuck_total} correct")

    with st.expander("Per-case STUCK detail"):
        st.dataframe(pd.DataFrame(stuck_detail), use_container_width=True, hide_index=True)

    # --- Aggregate SLOW count comparison ---
    st.subheader("Aggregate SLOW Counts: Truth vs Detected")

    gt_agg_slow = Counter()
    for _, row in gt_df.iterrows():
        for d in row["delayed_stages"]:
            gt_agg_slow[d] += 1

    all_steps = sorted(set(list(gt_agg_slow.keys()) + list(slow_counter.keys())),
                       key=lambda s: _STAGE_SEQ.index(s) if s in _STAGE_SEQ else 99)
    agg_rows = []
    for s in all_steps:
        gt_c = gt_agg_slow.get(s, 0)
        det_c = slow_counter.get(s, 0)
        agg_rows.append({
            "Step": s,
            "Truth": gt_c,
            "Detected": det_c,
            "Match": "YES" if gt_c == det_c else "NO",
            "Difference": det_c - gt_c,
        })
    agg_df = pd.DataFrame(agg_rows)
    st.dataframe(agg_df, use_container_width=True, hide_index=True)

    # Side-by-side bar chart
    fig_cmp = go.Figure()
    fig_cmp.add_trace(go.Bar(
        x=[r["Step"] for r in agg_rows],
        y=[r["Truth"] for r in agg_rows],
        name="Ground Truth",
        marker_color="#3498db",
    ))
    fig_cmp.add_trace(go.Bar(
        x=[r["Step"] for r in agg_rows],
        y=[r["Detected"] for r in agg_rows],
        name="Detected",
        marker_color="#e74c3c",
    ))
    fig_cmp.update_layout(
        barmode="group",
        title="SLOW Counts: Ground Truth vs Detected",
        height=350,
    )
    st.plotly_chart(fig_cmp, use_container_width=True)

    # --- STUCK aggregate ---
    st.subheader("Aggregate STUCK Counts: Truth vs Detected")

    gt_agg_stuck = Counter()
    for _, row in gt_df.iterrows():
        gt_agg_stuck[row["stuck_stage"]] += 1

    all_stuck = sorted(set(list(gt_agg_stuck.keys()) + list(stuck_counter.keys())),
                       key=lambda s: _STAGE_SEQ.index(s) if s in _STAGE_SEQ else 99)
    stuck_rows = []
    for s in all_stuck:
        gt_c = gt_agg_stuck.get(s, 0)
        det_c = stuck_counter.get(s, 0)
        stuck_rows.append({
            "Step": s,
            "Truth": gt_c,
            "Detected": det_c,
            "Match": "YES" if gt_c == det_c else "NO",
        })
    st.dataframe(pd.DataFrame(stuck_rows), use_container_width=True, hide_index=True)

    fig_stuck_cmp = go.Figure()
    fig_stuck_cmp.add_trace(go.Bar(
        x=[r["Step"] for r in stuck_rows],
        y=[r["Truth"] for r in stuck_rows],
        name="Ground Truth", marker_color="#3498db",
    ))
    fig_stuck_cmp.add_trace(go.Bar(
        x=[r["Step"] for r in stuck_rows],
        y=[r["Detected"] for r in stuck_rows],
        name="Detected", marker_color="#e74c3c",
    ))
    fig_stuck_cmp.update_layout(barmode="group", title="STUCK Counts: Truth vs Detected", height=350)
    st.plotly_chart(fig_stuck_cmp, use_container_width=True)

    # --- Co-occurrence pattern accuracy ---
    st.subheader("Correlation Pattern Accuracy")

    # Ground truth co-occurrences
    gt_pairs = Counter()
    for _, row in gt_df.iterrows():
        for d in row["delayed_stages"]:
            gt_pairs[(f"SLOW:{d}", f"STUCK:{row['stuck_stage']}")] += 1
        dlist = sorted(row["delayed_stages"], key=lambda s: _STAGE_SEQ.index(s))
        for i in range(len(dlist)):
            for j in range(i + 1, len(dlist)):
                gt_pairs[(f"SLOW:{dlist[i]}", f"SLOW:{dlist[j]}")] += 1

    # Detected 2-token patterns
    det_pairs = {}
    for r in stage_results:
        if len(r["pattern"]) == 2:
            det_pairs[tuple(r["pattern"])] = r["support_count"]

    gt_above = {k: v for k, v in gt_pairs.items() if v >= min_support}
    det_above = {k: v for k, v in det_pairs.items() if v >= min_support}

    all_pairs = sorted(set(list(gt_above.keys()) + list(det_above.keys())), key=lambda x: -(gt_above.get(x, 0)))
    pair_rows = []
    pair_tp = pair_fp = pair_fn = 0
    for pair in all_pairs:
        gt_c = gt_above.get(pair, 0)
        det_c = det_above.get(pair, 0)
        if gt_c > 0 and det_c == gt_c:
            status = "EXACT"
            pair_tp += 1
        elif gt_c > 0 and det_c > 0:
            status = "COUNT DIFF"
            pair_tp += 1
        elif gt_c > 0 and det_c == 0:
            status = "MISSED"
            pair_fn += 1
        else:
            status = "FALSE POS"
            pair_fp += 1
        pair_rows.append({
            "Cause": pair[0],
            "Effect": pair[1],
            "Truth Count": gt_c,
            "Detected Count": det_c,
            "Status": status,
        })

    pair_prec = pair_tp / (pair_tp + pair_fp) if (pair_tp + pair_fp) > 0 else 0
    pair_rec = pair_tp / (pair_tp + pair_fn) if (pair_tp + pair_fn) > 0 else 0
    pair_f1 = 2 * pair_prec * pair_rec / (pair_prec + pair_rec) if (pair_prec + pair_rec) > 0 else 0

    cp1, cp2, cp3 = st.columns(3)
    cp1.metric("Pattern Precision", f"{pair_prec:.1%}")
    cp2.metric("Pattern Recall", f"{pair_rec:.1%}")
    cp3.metric("Pattern F1", f"{pair_f1:.1%}")

    st.dataframe(pd.DataFrame(pair_rows), use_container_width=True, hide_index=True)

    # Summary verdict
    st.markdown("---")
    exact_counts = sum(1 for r in pair_rows if r["Status"] == "EXACT")
    total_checked = len(pair_rows)
    if exact_counts == total_checked and total_checked > 0:
        st.success(f"PERFECT: All {total_checked} correlation patterns matched exactly!")
    elif pair_f1 >= 0.8:
        st.info(f"GOOD: {exact_counts}/{total_checked} patterns matched exactly (F1={pair_f1:.1%})")
    else:
        st.warning(f"PARTIAL: {exact_counts}/{total_checked} patterns matched (F1={pair_f1:.1%})")
