"""
full_general_correlation_finder.py -- Generalized Delay Correlation Engine
==========================================================================

This file contains the general-purpose correlation mining engine.
It imports the static extraction query and DB connection from query.py,
then discovers the pipeline, baselines, and patterns automatically.

Usage
-----
    python full_general_correlation_finder.py
"""

from __future__ import annotations

import json
from collections import defaultdict, Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

try:
    from prefixspan import PrefixSpan
except ImportError:
    print("ERROR: prefixspan not installed. Run: pip install prefixspan")
    raise SystemExit(1)

from query import load_env_auto, neo4j_driver, extract_cases


# ==============================================================================
# HYPERPARAMETERS
# ==============================================================================

MIN_SUPPORT_FRAC = 0.05     # Minimum fraction of cases a pattern must appear in
SLOW_MULTIPLIER = 3.0       # A transition is SLOW if it exceeds this * median


# ==============================================================================
# HELPERS
# ==============================================================================

def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    ts = ts.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(ts)
    except ValueError:
        return None


def _minutes_between(a: Optional[str], b: Optional[str]) -> Optional[float]:
    dt_a = _parse_iso(a)
    dt_b = _parse_iso(b)
    if dt_a is None or dt_b is None:
        return None
    return (dt_b - dt_a).total_seconds() / 60.0


def _median(values: List[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    if n % 2 == 1:
        return s[n // 2]
    return (s[n // 2 - 1] + s[n // 2]) / 2.0


def _fmt_time(minutes: float) -> str:
    if abs(minutes) >= 1440:
        return f"{minutes / 1440:.1f} days"
    if abs(minutes) >= 60:
        return f"{minutes / 60:.1f} hrs"
    return f"{minutes:.0f} min"


# ==============================================================================
# STEP 1 -- DISCOVER THE PIPELINE FROM DATA
# ==============================================================================

def discover_pipeline(cases: List[Dict]) -> Tuple[
    List[str],                          # canonical_event_order
    List[Tuple[str, str, str]],         # transitions: (step_name, from_event, to_event)
    Dict[str, Optional[str]],           # blocking_point_map: last_event -> step_name
    List[str],                          # drill_fields
]:
    """
    Observe all cases and discover the canonical event ordering,
    transitions, blocking points, and drill-down fields.
    """
    # --- Discover event order ---
    event_positions: Dict[str, List[int]] = defaultdict(list)
    all_event_types: set = set()

    for case in cases:
        events = case.get("events") or []
        sorted_events = sorted(events, key=lambda e: e["event_time"] or "")
        for pos, ev in enumerate(sorted_events):
            etype = ev["event_type"]
            if etype:
                event_positions[etype].append(pos)
                all_event_types.add(etype)

    median_positions = {
        etype: _median(positions)
        for etype, positions in event_positions.items()
    }
    canonical_order = sorted(all_event_types, key=lambda e: median_positions.get(e, 999))

    # --- Discover transitions ---
    pair_counts: Counter = Counter()
    for case in cases:
        events = case.get("events") or []
        sorted_events = sorted(events, key=lambda e: e["event_time"] or "")
        etypes = [ev["event_type"] for ev in sorted_events if ev["event_type"]]
        for i in range(len(etypes) - 1):
            pair_counts[(etypes[i], etypes[i + 1])] += 1

    transitions: List[Tuple[str, str, str]] = []
    for i in range(len(canonical_order) - 1):
        ev_from = canonical_order[i]
        ev_to = canonical_order[i + 1]
        if pair_counts.get((ev_from, ev_to), 0) > 0:
            transitions.append((ev_from, ev_from, ev_to))

    # --- Build blocking point map ---
    blocking_point_map: Dict[str, Optional[str]] = {}

    terminal_event = canonical_order[-1] if canonical_order else None
    terminal_has_open = False
    terminal_has_completed = False
    for case in cases:
        events = case.get("events") or []
        if not events:
            continue
        sorted_events = sorted(events, key=lambda e: e["event_time"] or "")
        last_evt = sorted_events[-1]["event_type"] if sorted_events else None
        if last_evt == terminal_event:
            if case["case_status"] == "OPEN":
                terminal_has_open = True
            else:
                terminal_has_completed = True

    for ev in canonical_order:
        if ev == terminal_event:
            if terminal_has_open and not terminal_has_completed:
                blocking_point_map[ev] = ev
            elif terminal_has_open and terminal_has_completed:
                blocking_point_map[ev] = ev
            else:
                blocking_point_map[ev] = None
        else:
            ev_idx = canonical_order.index(ev)
            found = False
            for t_step, t_from, t_to in transitions:
                t_from_idx = canonical_order.index(t_from)
                t_to_idx = canonical_order.index(t_to)
                if t_from_idx <= ev_idx < t_to_idx:
                    blocking_point_map[ev] = t_step
                    found = True
                    break
            if not found:
                blocking_point_map[ev] = transitions[-1][0] if transitions else ev

    # --- Discover drill-down fields ---
    field_counter: Counter = Counter()
    field_value_sets: Dict[str, set] = defaultdict(set)

    for case in cases:
        root_attrs = case.get("root_attributes") or "{}"
        if isinstance(root_attrs, str):
            try:
                root_attrs = json.loads(root_attrs)
            except (json.JSONDecodeError, TypeError):
                continue
        for key, val in root_attrs.items():
            if isinstance(val, str) and not key.startswith("scenario"):
                field_counter[key] += 1
                field_value_sets[key].add(val)

    n_cases = len(cases)
    drill_fields = sorted([
        field for field, count in field_counter.items()
        if count >= n_cases * 0.5
        and 1 < len(field_value_sets[field]) < n_cases
    ])

    return canonical_order, transitions, blocking_point_map, drill_fields


# ==============================================================================
# STEP 2 -- COMPUTE BASELINES AND FLAG SLOW TRANSITIONS
# ==============================================================================

def compute_baselines_and_flag(
    cases: List[Dict],
    transitions: List[Tuple[str, str, str]],
    blocking_point_map: Dict[str, Optional[str]],
    drill_fields: List[str],
) -> Tuple[Dict[str, float], List[Dict[str, Any]]]:
    """
    Compute median transition time for each step, flag transitions that
    exceed SLOW_MULTIPLIER * median, and identify the blocking point.
    """
    # First pass: collect transition times
    all_times: Dict[str, List[float]] = defaultdict(list)
    for case in cases:
        event_times: Dict[str, str] = {}
        for ev in (case.get("events") or []):
            if ev["event_type"] and ev["event_time"]:
                event_times[ev["event_type"]] = ev["event_time"]
        for step_name, ev_from, ev_to in transitions:
            dt = _minutes_between(event_times.get(ev_from), event_times.get(ev_to))
            if dt is not None and dt > 0:
                all_times[step_name].append(dt)

    baselines = {step: _median(times) for step, times in all_times.items()}

    # Second pass: annotate each case
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

        slow_steps: List[str] = []
        transition_times: Dict[str, float] = {}
        for step_name, ev_from, ev_to in transitions:
            dt = _minutes_between(event_times.get(ev_from), event_times.get(ev_to))
            if dt is not None and dt > 0:
                transition_times[step_name] = dt
                threshold = baselines.get(step_name, 0) * SLOW_MULTIPLIER
                if threshold > 0 and dt > threshold:
                    slow_steps.append(step_name)

        sorted_events = sorted(events, key=lambda e: e["event_time"] or "")
        last_biz = sorted_events[-1]["event_type"] if sorted_events else None
        blocking_point = blocking_point_map.get(last_biz) if last_biz else None

        if case["case_status"] != "OPEN":
            blocking_point = None

        first_event_time = sorted_events[0]["event_time"] if sorted_events else None
        last_event_time = sorted_events[-1]["event_time"] if sorted_events else None
        total_lead = _minutes_between(first_event_time, last_event_time)

        stall_minutes = None
        if case["case_status"] == "OPEN" and sorted_events:
            last_dt = _parse_iso(sorted_events[-1]["event_time"])
            if last_dt:
                stall_minutes = max(0.0, (datetime.now(timezone.utc) - last_dt).total_seconds() / 60.0)

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


# ==============================================================================
# STEP 3 -- TOKENISE
# ==============================================================================

def tokenise_cases(
    annotated: List[Dict[str, Any]],
    transitions: List[Tuple[str, str, str]],
) -> Tuple[List[List[str]], List[Dict[str, Any]]]:
    sequences: List[List[str]] = []
    cases_out: List[Dict[str, Any]] = []

    for ac in annotated:
        tokens: List[str] = []
        for step_name, _, _ in transitions:
            if step_name in ac["slow_steps"]:
                tokens.append(f"SLOW:{step_name}")
        bp = ac["blocking_point"]
        if bp is not None:
            tokens.append(f"STUCK:{bp}")
        if tokens:
            sequences.append(tokens)
            cases_out.append(ac)

    return sequences, cases_out


def tokenise_with_field(
    annotated: List[Dict[str, Any]],
    transitions: List[Tuple[str, str, str]],
    field: str,
) -> Tuple[List[List[str]], List[Dict[str, Any]]]:
    sequences: List[List[str]] = []
    cases_out: List[Dict[str, Any]] = []

    for ac in annotated:
        val = ac["attrs"].get(field, "UNKNOWN")
        tokens: List[str] = []
        for step_name, _, _ in transitions:
            if step_name in ac["slow_steps"]:
                tokens.append(f"SLOW:{step_name}@{val}")
        bp = ac["blocking_point"]
        if bp is not None:
            tokens.append(f"STUCK:{bp}@{val}")
        if tokens:
            sequences.append(tokens)
            cases_out.append(ac)

    return sequences, cases_out


# ==============================================================================
# STEP 4 -- MINE WITH PREFIXSPAN
# ==============================================================================

def mine_patterns(
    sequences: List[List[str]],
    min_support: int,
) -> List[Tuple[int, List[str]]]:
    ps = PrefixSpan(sequences)
    results = ps.frequent(min_support, closed=False)
    results.sort(key=lambda x: -x[0])
    return results


# ==============================================================================
# STEP 5 -- FILTER FOR CORRELATION PATTERNS
# ==============================================================================

def _is_subsequence(pattern: List[str], seq: List[str]) -> bool:
    it = iter(seq)
    return all(tok in it for tok in pattern)


def filter_correlation_patterns(
    mined: List[Tuple[int, List[str]]],
    sequences: List[List[str]],
    annotated: List[Dict[str, Any]],
    total_cases: int,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    for support_count, pattern in mined:
        slow_tokens = [t for t in pattern if "SLOW:" in t]
        stuck_tokens = [t for t in pattern if "STUCK:" in t]

        if not slow_tokens:
            continue
        if not stuck_tokens and len(slow_tokens) < 2:
            continue

        matching_cases = []
        for i, seq in enumerate(sequences):
            if _is_subsequence(pattern, seq):
                matching_cases.append(annotated[i])

        stalls = [mc["stall_minutes"] for mc in matching_cases if mc.get("stall_minutes")]
        leads = [mc["total_lead_minutes"] for mc in matching_cases if mc.get("total_lead_minutes")]
        avg_stall = sum(stalls) / len(stalls) if stalls else 0.0
        avg_lead = sum(leads) / len(leads) if leads else 0.0

        field_dist: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for mc in matching_cases:
            for field, val in mc["attrs"].items():
                field_dist[field][val] += 1

        top_holders: Dict[str, List[Dict]] = {}
        for field, counts in field_dist.items():
            sorted_vals = sorted(counts.items(), key=lambda x: -x[1])
            top_holders[field] = [
                {"value": v, "count": c, "pct": round(c / len(matching_cases) * 100, 1)}
                for v, c in sorted_vals[:5]
            ]

        cause_tokens = list(slow_tokens)
        effect_tokens = list(stuck_tokens)
        if not effect_tokens and len(cause_tokens) >= 2:
            effect_tokens = [cause_tokens.pop()]

        support_frac = support_count / total_cases if total_cases > 0 else 0.0
        impact = avg_stall / 60.0 if avg_stall > 0 else avg_lead / 60.0

        results.append({
            "pattern": pattern,
            "pattern_str": " -> ".join(pattern),
            "cause": cause_tokens,
            "effect": effect_tokens,
            "support_count": support_count,
            "support_frac": round(support_frac, 4),
            "avg_stall_hours": round(avg_stall / 60, 2),
            "avg_stall_days": round(avg_stall / 1440, 2),
            "avg_lead_hours": round(avg_lead / 60, 2),
            "rank_score": round(support_frac * impact, 4),
            "sample_cases": [mc["case_id"] for mc in matching_cases[:5]],
            "top_holders": top_holders,
        })

    results.sort(key=lambda r: -r["rank_score"])
    return results


# ==============================================================================
# STEP 6 -- FIELD-ENRICHED MINING
# ==============================================================================

def mine_field_patterns(
    annotated: List[Dict[str, Any]],
    transitions: List[Tuple[str, str, str]],
    drill_fields: List[str],
    min_support: int,
    total_cases: int,
) -> List[Dict[str, Any]]:
    all_results: List[Dict[str, Any]] = []

    for field in drill_fields:
        seqs, cases = tokenise_with_field(annotated, transitions, field)
        if not seqs:
            continue
        mined = mine_patterns(seqs, min_support)
        filtered = filter_correlation_patterns(mined, seqs, cases, total_cases)
        for r in filtered:
            r["enrichment_field"] = field
            all_results.append(r)

    all_results.sort(key=lambda r: -r["rank_score"])
    return all_results


# ==============================================================================
# STEP 7 -- PRINT REPORT
# ==============================================================================

def print_report(
    canonical_order: List[str],
    transitions: List[Tuple[str, str, str]],
    blocking_point_map: Dict[str, Optional[str]],
    drill_fields: List[str],
    baselines: Dict[str, float],
    stage_results: List[Dict[str, Any]],
    field_results: List[Dict[str, Any]],
    total_cases: int,
    annotated: List[Dict[str, Any]],
) -> None:
    print()
    print("=" * 90)
    print("  GENERALIZED UPSTREAM/DOWNSTREAM DELAY CORRELATION ANALYSIS")
    print("=" * 90)
    print(f"  Total cases:               {total_cases}")
    print(f"  Min support fraction:      {MIN_SUPPORT_FRAC:.0%}")
    print(f"  Slow multiplier:           {SLOW_MULTIPLIER}x median")

    print()
    print("  DISCOVERED PIPELINE (canonical event order)")
    print("  " + "-" * 60)
    for i, ev in enumerate(canonical_order):
        bp = blocking_point_map.get(ev, "?")
        bp_label = f"  blocks at: {bp}" if bp else "  (terminal)"
        print(f"  {i+1:>3}. {ev:<30} {bp_label}")

    print()
    print("  DISCOVERED TRANSITIONS")
    print("  " + "-" * 60)
    for step_name, ev_from, ev_to in transitions:
        print(f"  {step_name:<30} {ev_from} -> {ev_to}")

    print()
    print(f"  DISCOVERED DRILL-DOWN FIELDS: {', '.join(drill_fields)}")

    print()
    print("  TRANSITION BASELINES (median times)")
    print("  " + "-" * 60)
    print(f"  {'Transition':<30} {'Median':>12} {'Threshold':>12}")
    print("  " + "-" * 60)
    for step_name, _, _ in transitions:
        med = baselines.get(step_name, 0)
        thresh = med * SLOW_MULTIPLIER
        print(f"  {step_name:<30} {_fmt_time(med):>12} {_fmt_time(thresh):>12}")

    slow_counter: Counter = Counter()
    stuck_counter: Counter = Counter()
    for ac in annotated:
        for s in ac["slow_steps"]:
            slow_counter[s] += 1
        if ac["blocking_point"]:
            stuck_counter[ac["blocking_point"]] += 1

    print()
    print("  DELAY DISTRIBUTION")
    print("  " + "-" * 50)
    for step_name, _, _ in transitions:
        sc = slow_counter.get(step_name, 0)
        stc = stuck_counter.get(step_name, 0)
        line = f"  {step_name:<30} SLOW: {sc:>3}"
        if stc > 0:
            line += f"   STUCK: {stc:>3}"
        print(line)

    if stage_results:
        print()
        print()
        print("  STAGE-LEVEL CORRELATION PATTERNS (ranked by support * impact)")
        print("  " + "-" * 86)
        for i, r in enumerate(stage_results, 1):
            cause_str = ", ".join(r["cause"])
            effect_str = ", ".join(r["effect"])
            print(f"  #{i}  {r['pattern_str']}")
            print(f"      Cause:    {cause_str}")
            print(f"      Effect:   {effect_str}")
            print(f"      Support:  {r['support_count']} cases ({r['support_frac']:.1%})")
            print(f"      Avg Stall: {_fmt_time(r['avg_stall_hours'] * 60)}")
            print(f"      Cases:    {', '.join(r['sample_cases'])}")
            print()
    else:
        print("\n  No stage-level correlation patterns found.")

    if field_results:
        print()
        print("  FIELD-ENRICHED CORRELATION PATTERNS (top 20)")
        print("  " + "-" * 86)
        for i, r in enumerate(field_results[:20], 1):
            print(f"  #{i}  [{r['enrichment_field']}]  {r['pattern_str']}")
            print(f"      Support: {r['support_count']} ({r['support_frac']:.1%})  "
                  f"Avg Stall: {_fmt_time(r['avg_stall_hours'] * 60)}  "
                  f"Cases: {', '.join(r['sample_cases'][:3])}")
            print()
    else:
        print("\n  No field-enriched correlation patterns found.")

    print("  " + "=" * 86)
    print()


# ==============================================================================
# MAIN
# ==============================================================================

def main() -> None:
    load_env_auto()

    print("Connecting to Neo4j...")
    driver = neo4j_driver()

    # -- 1. Extract (uses query.py) --
    print("Extracting cases from Neo4j...")
    cases = extract_cases(driver)
    driver.close()

    total_cases = len(cases)
    if total_cases == 0:
        print("No cases found.")
        return
    print(f"  Extracted {total_cases} cases.")

    # -- 2. Discover pipeline --
    print("Discovering pipeline structure from data...")
    canonical_order, transitions, blocking_point_map, drill_fields = discover_pipeline(cases)
    print(f"  Events: {len(canonical_order)}, Transitions: {len(transitions)}, Fields: {len(drill_fields)}")

    if not transitions:
        print("  No transitions discovered. Cannot proceed.")
        return

    # -- 3. Baseline & flag --
    print("Computing baselines and flagging slow transitions...")
    baselines, annotated = compute_baselines_and_flag(
        cases, transitions, blocking_point_map, drill_fields
    )

    # -- 4+5. Tokenise & mine --
    print("Tokenising and mining stage-level patterns...")
    sequences, seq_cases = tokenise_cases(annotated, transitions)
    min_support = max(2, int(len(sequences) * MIN_SUPPORT_FRAC))
    print(f"  {len(sequences)} sequences, min_support={min_support}")

    mined = mine_patterns(sequences, min_support)
    print(f"  PrefixSpan found {len(mined)} frequent subsequences.")

    # -- 6. Filter --
    stage_results = filter_correlation_patterns(mined, sequences, seq_cases, total_cases)
    print(f"  {len(stage_results)} correlation patterns after filtering.")

    # -- 7. Field-enriched --
    print("Mining field-enriched patterns...")
    field_results = mine_field_patterns(annotated, transitions, drill_fields, min_support, total_cases)
    print(f"  {len(field_results)} field-enriched correlation patterns.")

    # -- 8. Output --
    print_report(
        canonical_order, transitions, blocking_point_map, drill_fields,
        baselines, stage_results, field_results, total_cases, annotated,
    )

    output = {
        "meta": {
            "total_cases": total_cases,
            "min_support_frac": MIN_SUPPORT_FRAC,
            "slow_multiplier": SLOW_MULTIPLIER,
            "min_support_count": min_support,
        },
        "discovered_pipeline": {
            "canonical_event_order": canonical_order,
            "transitions": [
                {"step_name": s, "from_event": f, "to_event": t}
                for s, f, t in transitions
            ],
            "blocking_point_map": blocking_point_map,
            "drill_fields": drill_fields,
        },
        "baselines_minutes": {k: round(v, 2) for k, v in baselines.items()},
        "stage_correlation_patterns": stage_results,
        "field_correlation_patterns": field_results[:50],
    }

    out_path = Path(__file__).parent / "full_general_correlation_finder_results.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Full results written to: {out_path}")


if __name__ == "__main__":
    main()
