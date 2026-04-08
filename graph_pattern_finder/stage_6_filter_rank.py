"""
stage_6_filter_rank.py -- Filter mined patterns and rank by impact.

Takes the raw PrefixSpan output from stage 5, filters for correlation
patterns (upstream SLOW -> downstream SLOW/STUCK), computes impact
metrics, and ranks them.

Also handles field-enriched mining: re-runs the tokenise->mine->filter
pipeline for each drill-down field to find field-specific patterns.

Standalone usage:
    python -m graph_pattern_finder.stage_6_filter_rank

    Reads mined_patterns.json, annotated_cases.json, token_sequences.json,
    and discovered_pipeline.json. Writes correlation_results.json.

As a library:
    from graph_pattern_finder.stage_6_filter_rank import (
        filter_correlation_patterns,
        mine_field_patterns,
    )
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Any, Tuple

from graph_pattern_finder.stage_4_tokenise import tokenise_with_field
from graph_pattern_finder.stage_5_mine import mine_patterns


# Default minimum support fraction
DEFAULT_MIN_SUPPORT_FRAC = 0.05


def _is_subsequence(pattern: List[str], seq: List[str]) -> bool:
    """Check if pattern is a subsequence of seq (order preserved, gaps allowed)."""
    it = iter(seq)
    return all(tok in it for tok in pattern)


def filter_correlation_patterns(
    mined: List[Tuple[int, List[str]]],
    sequences: List[List[str]],
    annotated: List[Dict[str, Any]],
    total_cases: int,
) -> List[Dict[str, Any]]:
    """
    Filter mined patterns to keep only cause->effect correlations.

    A correlation pattern must have:
      - At least one SLOW: token (the upstream cause)
      - At least one STUCK: token OR a second SLOW: token (the downstream effect)

    For each kept pattern, computes:
      support_count, support_frac, avg_stall, avg_lead, rank_score,
      sample_cases, top_holders (field distributions), cause/effect tokens.

    Parameters
    ----------
    mined:        raw PrefixSpan output [(support, pattern), ...]
    sequences:    token sequences (same order as annotated)
    annotated:    annotated case dicts (from stage 3)
    total_cases:  total number of cases (denominator for support_frac)

    Returns
    -------
    List of result dicts, sorted by rank_score descending.
    """
    results: List[Dict[str, Any]] = []

    for support_count, pattern in mined:
        slow_tokens = [t for t in pattern if "SLOW:" in t]
        stuck_tokens = [t for t in pattern if "STUCK:" in t]

        # Must have at least one SLOW (the cause)
        if not slow_tokens:
            continue
        # Must have a downstream effect
        if not stuck_tokens and len(slow_tokens) < 2:
            continue

        # Find matching cases
        matching_cases = []
        for i, seq in enumerate(sequences):
            if _is_subsequence(pattern, seq):
                matching_cases.append(annotated[i])

        # Compute impact metrics
        stalls = [mc["stall_minutes"] for mc in matching_cases if mc.get("stall_minutes")]
        leads = [mc["total_lead_minutes"] for mc in matching_cases if mc.get("total_lead_minutes")]
        avg_stall = sum(stalls) / len(stalls) if stalls else 0.0
        avg_lead = sum(leads) / len(leads) if leads else 0.0

        # Field distributions for drill-down
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

        # Separate cause from effect
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


def mine_field_patterns(
    annotated: List[Dict[str, Any]],
    transitions: List[Tuple[str, str, str]],
    drill_fields: List[str],
    min_support: int,
    total_cases: int,
) -> List[Dict[str, Any]]:
    """
    Run the tokenise->mine->filter pipeline for each drill-down field.

    Returns field-enriched correlation patterns with an extra
    "enrichment_field" key, sorted by rank_score.
    """
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


# -- Standalone entry point ---------------------------------------------------

def main() -> None:
    base = Path(__file__).parent

    for needed in ["mined_patterns.json", "annotated_cases.json",
                   "token_sequences.json", "discovered_pipeline.json"]:
        if not (base / needed).exists():
            print(f"Missing {needed}. Run earlier stages first.")
            return

    print("Stage 6: Filtering and ranking correlation patterns...")

    with open(base / "mined_patterns.json") as f:
        mined_data = json.load(f)
    with open(base / "annotated_cases.json") as f:
        annotated = json.load(f)
    with open(base / "token_sequences.json") as f:
        tok_data = json.load(f)
    with open(base / "discovered_pipeline.json") as f:
        pipeline = json.load(f)

    sequences = tok_data["sequences"]
    mined = [(p["support"], p["pattern"]) for p in mined_data["patterns"]]
    total_cases = mined_data["total_sequences"]

    # Filter stage-level patterns
    stage_results = filter_correlation_patterns(mined, sequences, annotated, total_cases)
    print(f"  {len(stage_results)} stage-level correlation patterns.")

    # Field-enriched patterns
    transitions = [
        (t["step_name"], t["from_event"], t["to_event"])
        for t in pipeline["transitions"]
    ]
    drill_fields = pipeline["drill_fields"]
    min_support = mined_data["min_support"]

    field_results = mine_field_patterns(
        annotated, transitions, drill_fields, min_support, total_cases
    )
    print(f"  {len(field_results)} field-enriched correlation patterns.")

    out = {
        "stage_correlation_patterns": stage_results,
        "field_correlation_patterns": field_results[:50],
    }
    out_path = base / "correlation_results.json"
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"  Written to {out_path}")


if __name__ == "__main__":
    main()
