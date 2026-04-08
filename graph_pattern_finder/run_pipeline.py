"""
run_pipeline.py -- Orchestrator that runs all stages end-to-end.

Chains stages 1 through 6 in sequence and prints a full report.
This is equivalent to running full_general_correlation_finder.py
but using the modular stage files.

Usage:
    python -m graph_pattern_finder.run_pipeline

    Or from the project root:
    python graph_pattern_finder/run_pipeline.py
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

# Add parent dir so we can import query.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from graph_pattern_finder.helpers import fmt_time
from graph_pattern_finder.stage_1_extract import extract
from graph_pattern_finder.stage_2_discover import discover_pipeline
from graph_pattern_finder.stage_3_baseline import compute_baselines_and_flag
from graph_pattern_finder.stage_4_tokenise import tokenise_cases
from graph_pattern_finder.stage_5_mine import mine_patterns
from graph_pattern_finder.stage_6_filter_rank import (
    filter_correlation_patterns,
    mine_field_patterns,
)


# ==============================================================================
# HYPERPARAMETERS
# ==============================================================================

MIN_SUPPORT_FRAC = 0.05
SLOW_MULTIPLIER = 3.0


# ==============================================================================
# REPORT
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
        print(f"  {step_name:<30} {fmt_time(med):>12} {fmt_time(thresh):>12}")

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
            print(f"      Avg Stall: {fmt_time(r['avg_stall_hours'] * 60)}")
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
                  f"Avg Stall: {fmt_time(r['avg_stall_hours'] * 60)}  "
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
    # -- Stage 1: Extract --
    print("Stage 1: Extracting cases from Neo4j...")
    cases = extract()
    total_cases = len(cases)
    if total_cases == 0:
        print("No cases found.")
        return
    print(f"  Extracted {total_cases} cases.")

    # -- Stage 2: Discover --
    print("Stage 2: Discovering pipeline structure...")
    canonical_order, transitions, blocking_point_map, drill_fields = discover_pipeline(cases)
    print(f"  Events: {len(canonical_order)}, Transitions: {len(transitions)}, Fields: {len(drill_fields)}")
    if not transitions:
        print("  No transitions discovered. Cannot proceed.")
        return

    # -- Stage 3: Baseline --
    print("Stage 3: Computing baselines and flagging slow transitions...")
    baselines, annotated = compute_baselines_and_flag(
        cases, transitions, blocking_point_map, drill_fields, SLOW_MULTIPLIER
    )

    # -- Stage 4+5: Tokenise & Mine --
    print("Stage 4: Tokenising...")
    sequences, seq_cases = tokenise_cases(annotated, transitions)
    min_support = max(2, int(len(sequences) * MIN_SUPPORT_FRAC))
    print(f"  {len(sequences)} sequences, min_support={min_support}")

    print("Stage 5: Mining with PrefixSpan...")
    mined = mine_patterns(sequences, min_support)
    print(f"  {len(mined)} frequent subsequences.")

    # -- Stage 6: Filter & Rank --
    print("Stage 6: Filtering correlation patterns...")
    stage_results = filter_correlation_patterns(mined, sequences, seq_cases, total_cases)
    print(f"  {len(stage_results)} stage-level patterns.")

    field_results = mine_field_patterns(annotated, transitions, drill_fields, min_support, total_cases)
    print(f"  {len(field_results)} field-enriched patterns.")

    # -- Report --
    print_report(
        canonical_order, transitions, blocking_point_map, drill_fields,
        baselines, stage_results, field_results, total_cases, annotated,
    )

    # -- Write JSON --
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

    out_path = Path(__file__).parent / "pipeline_results.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"Full results written to: {out_path}")


if __name__ == "__main__":
    main()
