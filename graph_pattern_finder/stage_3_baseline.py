"""
stage_3_baseline.py -- Compute timing baselines and flag slow transitions.

For each transition in the discovered pipeline, computes the median
duration across all cases. Then annotates every case with:
  - Which transitions were SLOW (exceeded SLOW_MULTIPLIER * median)
  - Where the case is stuck (blocking point)
  - Stall time, lead time, and drill-down field values

Standalone usage:
    python -m graph_pattern_finder.stage_3_baseline

    Reads extracted_cases.json and discovered_pipeline.json, writes
    baselines.json and annotated_cases.json.

As a library:
    from graph_pattern_finder.stage_3_baseline import compute_baselines_and_flag
    baselines, annotated = compute_baselines_and_flag(
        cases, transitions, blocking_point_map, drill_fields,
        slow_multiplier=3.0,
    )
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

from graph_pattern_finder.helpers import parse_iso, minutes_between, median


# Default: a transition is SLOW if it exceeds 3x the median
DEFAULT_SLOW_MULTIPLIER = 3.0


def compute_baselines_and_flag(
    cases: List[Dict],
    transitions: List[Tuple[str, str, str]],
    blocking_point_map: Dict[str, Optional[str]],
    drill_fields: List[str],
    slow_multiplier: float = DEFAULT_SLOW_MULTIPLIER,
) -> Tuple[Dict[str, float], List[Dict[str, Any]]]:
    """
    Compute median transition times and annotate each case.

    Parameters
    ----------
    cases:              raw case dicts from stage 1
    transitions:        (step_name, from_event, to_event) tuples from stage 2
    blocking_point_map: event -> blocking step name, from stage 2
    drill_fields:       categorical field names from stage 2
    slow_multiplier:    threshold multiplier (default 3.0)

    Returns
    -------
    baselines:  dict of step_name -> median duration in minutes
    annotated:  list of annotated case dicts, each containing:
                  case_id, case_status, slow_steps, blocking_point,
                  transition_times, total_lead_minutes, stall_minutes,
                  attrs, scenario_type
    """
    # -- First pass: collect all transition durations --
    all_times: Dict[str, List[float]] = defaultdict(list)
    for case in cases:
        event_times: Dict[str, str] = {}
        for ev in (case.get("events") or []):
            if ev["event_type"] and ev["event_time"]:
                event_times[ev["event_type"]] = ev["event_time"]
        for step_name, ev_from, ev_to in transitions:
            dt = minutes_between(event_times.get(ev_from), event_times.get(ev_to))
            if dt is not None and dt > 0:
                all_times[step_name].append(dt)

    baselines = {step: median(times) for step, times in all_times.items()}

    # -- Second pass: annotate each case --
    annotated: List[Dict[str, Any]] = []
    for case in cases:
        events = case.get("events") or []
        if not events:
            continue

        event_times: Dict[str, str] = {}
        for ev in events:
            if ev["event_type"] and ev["event_time"]:
                event_times[ev["event_type"]] = ev["event_time"]

        root_attrs = case.get("root_attributes") or "{}"
        if isinstance(root_attrs, str):
            try:
                root_attrs = json.loads(root_attrs)
            except (json.JSONDecodeError, TypeError):
                root_attrs = {}

        # Flag slow transitions
        slow_steps: List[str] = []
        transition_times: Dict[str, float] = {}
        for step_name, ev_from, ev_to in transitions:
            dt = minutes_between(event_times.get(ev_from), event_times.get(ev_to))
            if dt is not None and dt > 0:
                transition_times[step_name] = dt
                threshold = baselines.get(step_name, 0) * slow_multiplier
                if threshold > 0 and dt > threshold:
                    slow_steps.append(step_name)

        # Determine blocking point
        sorted_events = sorted(events, key=lambda e: e["event_time"] or "")
        last_biz = sorted_events[-1]["event_type"] if sorted_events else None
        blocking_point = blocking_point_map.get(last_biz) if last_biz else None
        if case["case_status"] != "OPEN":
            blocking_point = None

        # Lead and stall times
        first_time = sorted_events[0]["event_time"] if sorted_events else None
        last_time = sorted_events[-1]["event_time"] if sorted_events else None
        total_lead = minutes_between(first_time, last_time)

        stall_minutes = None
        if case["case_status"] == "OPEN" and sorted_events:
            last_dt = parse_iso(sorted_events[-1]["event_time"])
            if last_dt:
                stall_minutes = max(
                    0.0,
                    (datetime.now(timezone.utc) - last_dt).total_seconds() / 60.0,
                )

        # Extract drill-down field values
        attrs: Dict[str, str] = {}
        for field in drill_fields:
            attrs[field] = str(root_attrs.get(field, "UNKNOWN"))

        annotated.append({
            "case_id": case["case_id"],
            "case_status": case["case_status"],
            "slow_steps": slow_steps,
            "blocking_point": blocking_point,
            "transition_times": transition_times,
            "total_lead_minutes": total_lead,
            "stall_minutes": stall_minutes,
            "attrs": attrs,
            "scenario_type": root_attrs.get("scenario_type", ""),
        })

    return baselines, annotated


# -- Standalone entry point ---------------------------------------------------

def main() -> None:
    base = Path(__file__).parent
    cases_path = base / "extracted_cases.json"
    pipeline_path = base / "discovered_pipeline.json"

    if not cases_path.exists() or not pipeline_path.exists():
        print("Run stages 1 and 2 first.")
        return

    print("Stage 3: Computing baselines and flagging slow transitions...")
    with open(cases_path) as f:
        cases = json.load(f)
    with open(pipeline_path) as f:
        pipeline = json.load(f)

    transitions = [
        (t["step_name"], t["from_event"], t["to_event"])
        for t in pipeline["transitions"]
    ]
    blocking_point_map = pipeline["blocking_point_map"]
    drill_fields = pipeline["drill_fields"]

    baselines, annotated = compute_baselines_and_flag(
        cases, transitions, blocking_point_map, drill_fields
    )

    slow_count = sum(len(ac["slow_steps"]) for ac in annotated)
    stuck_count = sum(1 for ac in annotated if ac["blocking_point"])
    print(f"  {len(annotated)} cases annotated, {slow_count} slow flags, {stuck_count} stuck")

    with open(base / "baselines.json", "w") as f:
        json.dump(baselines, f, indent=2)
    with open(base / "annotated_cases.json", "w") as f:
        json.dump(annotated, f, indent=2)
    print(f"  Written baselines.json and annotated_cases.json")


if __name__ == "__main__":
    main()
