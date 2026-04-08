"""
logger.py -- Stage-level JSON logger for graph_pattern_finder.

Wraps each pipeline stage, runs it, captures the output, and writes
a timestamped JSON log file. Each log contains:
  - stage name and timestamp
  - input summary (shapes/counts of input data)
  - full output data
  - duration in seconds

Usage as a library:
    from graph_pattern_finder.logger.logger import LoggedPipeline
    lp = LoggedPipeline(log_dir="my_logs")
    cases = lp.extract()
    pipeline = lp.discover(cases)
    ...

Standalone usage (runs full pipeline with logging):
    python -m graph_pattern_finder.logger.logger
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from graph_pattern_finder.stage_1_extract import extract
from graph_pattern_finder.stage_2_discover import discover_pipeline
from graph_pattern_finder.stage_3_baseline import compute_baselines_and_flag
from graph_pattern_finder.stage_4_tokenise import tokenise_cases
from graph_pattern_finder.stage_5_mine import mine_patterns
from graph_pattern_finder.stage_6_filter_rank import (
    filter_correlation_patterns,
    mine_field_patterns,
)


DEFAULT_MIN_SUPPORT_FRAC = 0.05
DEFAULT_SLOW_MULTIPLIER = 3.0


def _write_log(log_dir: Path, stage_name: str, payload: Dict[str, Any]) -> Path:
    """Write a JSON log file and return the path."""
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"{stage_name}_{ts}.json"
    path = log_dir / filename
    with open(path, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    return path


class LoggedPipeline:
    """
    Wraps every graph_pattern_finder stage with JSON logging.

    Each method runs the stage, logs inputs/outputs to a JSON file,
    and returns the same values the underlying stage returns.
    """

    def __init__(
        self,
        log_dir: str | Path = "logs",
        min_support_frac: float = DEFAULT_MIN_SUPPORT_FRAC,
        slow_multiplier: float = DEFAULT_SLOW_MULTIPLIER,
    ):
        self.log_dir = Path(log_dir)
        self.min_support_frac = min_support_frac
        self.slow_multiplier = slow_multiplier

    # -- Stage 1: Extract --------------------------------------------------

    def extract(self) -> List[Dict]:
        t0 = time.time()
        cases = extract()
        duration = time.time() - t0

        log = {
            "stage": "stage_1_extract",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": round(duration, 3),
            "input": {"source": "neo4j"},
            "output": {
                "total_cases": len(cases),
                "open_cases": sum(1 for c in cases if c.get("case_status") == "OPEN"),
                "sample_case_ids": [c["case_id"] for c in cases[:5]],
            },
        }
        path = _write_log(self.log_dir, "stage_1_extract", log)
        print(f"  [LOG] Stage 1 -> {path}")
        return cases

    # -- Stage 2: Discover -------------------------------------------------

    def discover(self, cases: List[Dict]) -> Tuple[
        List[str], List[Tuple[str, str, str]], Dict[str, Optional[str]], List[str]
    ]:
        t0 = time.time()
        canonical_order, transitions, blocking_point_map, drill_fields = discover_pipeline(cases)
        duration = time.time() - t0

        log = {
            "stage": "stage_2_discover",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": round(duration, 3),
            "input": {"total_cases": len(cases)},
            "output": {
                "canonical_event_order": canonical_order,
                "transitions": [
                    {"step_name": s, "from_event": f, "to_event": t}
                    for s, f, t in transitions
                ],
                "blocking_point_map": blocking_point_map,
                "drill_fields": drill_fields,
            },
        }
        path = _write_log(self.log_dir, "stage_2_discover", log)
        print(f"  [LOG] Stage 2 -> {path}")
        return canonical_order, transitions, blocking_point_map, drill_fields

    # -- Stage 3: Baseline -------------------------------------------------

    def baseline(
        self,
        cases: List[Dict],
        transitions: List[Tuple[str, str, str]],
        blocking_point_map: Dict[str, Optional[str]],
        drill_fields: List[str],
    ) -> Tuple[Dict[str, float], List[Dict[str, Any]]]:
        t0 = time.time()
        baselines, annotated = compute_baselines_and_flag(
            cases, transitions, blocking_point_map, drill_fields, self.slow_multiplier
        )
        duration = time.time() - t0

        from collections import Counter
        slow_counter = Counter()
        stuck_counter = Counter()
        for ac in annotated:
            for s in ac["slow_steps"]:
                slow_counter[s] += 1
            if ac["blocking_point"]:
                stuck_counter[ac["blocking_point"]] += 1

        log = {
            "stage": "stage_3_baseline",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": round(duration, 3),
            "input": {
                "total_cases": len(cases),
                "num_transitions": len(transitions),
                "slow_multiplier": self.slow_multiplier,
            },
            "output": {
                "baselines_minutes": {k: round(v, 2) for k, v in baselines.items()},
                "annotated_count": len(annotated),
                "slow_counts": dict(slow_counter),
                "stuck_counts": dict(stuck_counter),
            },
        }
        path = _write_log(self.log_dir, "stage_3_baseline", log)
        print(f"  [LOG] Stage 3 -> {path}")
        return baselines, annotated

    # -- Stage 4: Tokenise -------------------------------------------------

    def tokenise(
        self,
        annotated: List[Dict[str, Any]],
        transitions: List[Tuple[str, str, str]],
    ) -> Tuple[List[List[str]], List[Dict[str, Any]]]:
        t0 = time.time()
        sequences, cases_out = tokenise_cases(annotated, transitions)
        duration = time.time() - t0

        log = {
            "stage": "stage_4_tokenise",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": round(duration, 3),
            "input": {"annotated_count": len(annotated), "num_transitions": len(transitions)},
            "output": {
                "num_sequences": len(sequences),
                "avg_length": round(sum(len(s) for s in sequences) / max(len(sequences), 1), 2),
                "sample_sequences": sequences[:5],
            },
        }
        path = _write_log(self.log_dir, "stage_4_tokenise", log)
        print(f"  [LOG] Stage 4 -> {path}")
        return sequences, cases_out

    # -- Stage 5: Mine -----------------------------------------------------

    def mine(
        self,
        sequences: List[List[str]],
        min_support: Optional[int] = None,
    ) -> List[Tuple[int, List[str]]]:
        if min_support is None:
            min_support = max(2, int(len(sequences) * self.min_support_frac))

        t0 = time.time()
        mined = mine_patterns(sequences, min_support)
        duration = time.time() - t0

        log = {
            "stage": "stage_5_mine",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": round(duration, 3),
            "input": {"num_sequences": len(sequences), "min_support": min_support},
            "output": {
                "num_patterns": len(mined),
                "patterns": [
                    {"support": sup, "pattern": pat} for sup, pat in mined[:30]
                ],
            },
        }
        path = _write_log(self.log_dir, "stage_5_mine", log)
        print(f"  [LOG] Stage 5 -> {path}")
        return mined

    # -- Stage 6: Filter & Rank --------------------------------------------

    def filter_and_rank(
        self,
        mined: List[Tuple[int, List[str]]],
        sequences: List[List[str]],
        annotated: List[Dict[str, Any]],
        total_cases: int,
        transitions: List[Tuple[str, str, str]],
        drill_fields: List[str],
        min_support: Optional[int] = None,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        if min_support is None:
            min_support = max(2, int(len(sequences) * self.min_support_frac))

        t0 = time.time()
        stage_results = filter_correlation_patterns(mined, sequences, annotated, total_cases)
        field_results = mine_field_patterns(
            annotated, transitions, drill_fields, min_support, total_cases
        )
        duration = time.time() - t0

        log = {
            "stage": "stage_6_filter_rank",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": round(duration, 3),
            "input": {
                "num_mined": len(mined),
                "num_sequences": len(sequences),
                "total_cases": total_cases,
                "min_support": min_support,
            },
            "output": {
                "stage_patterns": len(stage_results),
                "field_patterns": len(field_results),
                "stage_correlation_patterns": stage_results,
                "field_correlation_patterns": field_results[:30],
            },
        }
        path = _write_log(self.log_dir, "stage_6_filter_rank", log)
        print(f"  [LOG] Stage 6 -> {path}")
        return stage_results, field_results

    # -- Full pipeline (convenience) ---------------------------------------

    def run_all(self) -> Dict[str, Any]:
        """Run the full pipeline with logging at every stage."""
        print("Running full logged pipeline...")

        cases = self.extract()
        if not cases:
            return {"error": "No cases found"}

        canonical_order, transitions, blocking_point_map, drill_fields = self.discover(cases)
        if not transitions:
            return {"error": "No transitions discovered"}

        baselines, annotated = self.baseline(
            cases, transitions, blocking_point_map, drill_fields
        )

        sequences, seq_cases = self.tokenise(annotated, transitions)
        min_support = max(2, int(len(sequences) * self.min_support_frac))

        mined = self.mine(sequences, min_support)

        stage_results, field_results = self.filter_and_rank(
            mined, sequences, seq_cases, len(cases),
            transitions, drill_fields, min_support,
        )

        # Write a summary log
        summary = {
            "stage": "pipeline_summary",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_cases": len(cases),
            "min_support_frac": self.min_support_frac,
            "slow_multiplier": self.slow_multiplier,
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
        path = _write_log(self.log_dir, "pipeline_summary", summary)
        print(f"  [LOG] Summary -> {path}")

        return summary


# -- Standalone entry point ------------------------------------------------

def main() -> None:
    from query import load_env_auto
    load_env_auto()

    log_dir = Path(__file__).parent / "logs"
    lp = LoggedPipeline(log_dir=log_dir)
    result = lp.run_all()

    if "error" in result:
        print(f"ERROR: {result['error']}")
    else:
        print(f"\nDone. {result.get('total_cases', 0)} cases analysed.")
        print(f"Logs written to: {log_dir}")


if __name__ == "__main__":
    main()
