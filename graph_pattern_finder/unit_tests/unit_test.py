"""
unit_test.py -- Full integration test for graph_pattern_finder.

Loads scenario_multiple.py data into Neo4j, runs the logged pipeline,
and verifies results against the deterministic ground truth.

Hyperparameters at the top control the test scenario.

Usage:
    python -m graph_pattern_finder.unit_tests.unit_test
"""

from __future__ import annotations

import sys
import random
from collections import Counter
from pathlib import Path
from typing import Dict, List, Set, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

# ===========================================================================
# HYPERPARAMETERS -- change these to create different test scenarios
# ===========================================================================

NUM_CASES = 100           # Number of cases to generate
DELAY_PROB = 0.35         # Probability of delay at each stage
STUCK_WEIGHTS = [20, 20, 10, 15, 20, 15]  # Stuck-stage weights
MIN_SUPPORT_FRAC = 0.05   # Minimum support for pattern mining
SLOW_MULTIPLIER = 3.0     # Threshold for SLOW detection
BATCH_SIZE = 100           # DB batch size

# ===========================================================================
# IMPORTS
# ===========================================================================

from query import load_env_auto, neo4j_driver
from scenario_multiple import (
    _STAGE_SEQ,
    _rng,
    build_case_payload,
    create_constraints,
    upsert_open_case_batch,
    chunked,
)
import scenario_multiple as sm

from graph_pattern_finder.logger.logger import LoggedPipeline


# ===========================================================================
# GROUND TRUTH COMPUTATION
# ===========================================================================

def compute_ground_truth(
    num_cases: int,
    delay_prob: float,
    stuck_weights: List[int],
) -> Tuple[
    Dict[str, Set[str]],    # case_id -> set of delayed stages
    Dict[str, str],          # case_id -> stuck stage
    Counter,                 # (SLOW:X, STUCK:Y) -> count
]:
    """Replay the scenario_multiple RNG to get the exact ground truth."""
    gt_slow: Dict[str, Set[str]] = {}
    gt_stuck: Dict[str, str] = {}
    gt_pairs: Counter = Counter()

    for idx in range(1, num_cases + 1):
        rng = _rng(idx)
        stuck = rng.choices(_STAGE_SEQ, weights=stuck_weights, k=1)[0]
        stuck_idx = _STAGE_SEQ.index(stuck)
        delayed = []
        for stage in _STAGE_SEQ[:stuck_idx]:
            if rng.random() < delay_prob:
                delayed.append(stage)

        cid = f"CASE_{idx}"
        gt_slow[cid] = set(delayed)
        gt_stuck[cid] = stuck

        for d in delayed:
            gt_pairs[(f"SLOW:{d}", f"STUCK:{stuck}")] += 1
        for i in range(len(delayed)):
            for j in range(i + 1, len(delayed)):
                gt_pairs[(f"SLOW:{delayed[i]}", f"SLOW:{delayed[j]}")] += 1

    return gt_slow, gt_stuck, gt_pairs


# ===========================================================================
# LOAD DATA INTO NEO4J
# ===========================================================================

def load_data(num_cases: int, delay_prob: float, stuck_weights: List[int]) -> None:
    """Generate and load cases into Neo4j."""
    # Monkey-patch scenario_multiple module globals
    sm._DELAY_PROB = delay_prob
    sm._STUCK_WEIGHTS = stuck_weights

    load_env_auto()
    driver = neo4j_driver()
    with driver.session() as session:
        session.execute_write(create_constraints)
        all_ids = list(range(1, num_cases + 1))
        for batch_ids in chunked(all_ids, BATCH_SIZE):
            payloads = [build_case_payload(idx) for idx in batch_ids]
            session.execute_write(upsert_open_case_batch, payloads)
    driver.close()


# ===========================================================================
# VERIFICATION
# ===========================================================================

def verify(
    gt_slow: Dict[str, Set[str]],
    gt_stuck: Dict[str, str],
    gt_pairs: Counter,
    annotated: list,
    stage_results: list,
    min_support: int,
) -> Tuple[int, int, List[str]]:
    """
    Verify pipeline output against ground truth.
    Returns (checks_passed, total_checks, list_of_failure_messages).
    """
    passed = 0
    total = 0
    failures = []

    # -- Check 1: SLOW counts per step --
    gt_slow_counts = Counter()
    for cid, stages in gt_slow.items():
        for s in stages:
            gt_slow_counts[s] += 1

    det_slow_counts = Counter()
    for ac in annotated:
        for s in ac["slow_steps"]:
            det_slow_counts[s] += 1

    for s in _STAGE_SEQ:
        total += 1
        gt_c = gt_slow_counts.get(s, 0)
        det_c = det_slow_counts.get(s, 0)
        if gt_c == det_c:
            passed += 1
        else:
            failures.append(f"SLOW count {s}: truth={gt_c}, detected={det_c}")

    # -- Check 2: STUCK counts per step --
    gt_stuck_counts = Counter()
    for cid, s in gt_stuck.items():
        gt_stuck_counts[s] += 1

    det_stuck_counts = Counter()
    for ac in annotated:
        if ac["blocking_point"]:
            det_stuck_counts[ac["blocking_point"]] += 1

    for s in _STAGE_SEQ:
        total += 1
        gt_c = gt_stuck_counts.get(s, 0)
        det_c = det_stuck_counts.get(s, 0)
        if gt_c == det_c:
            passed += 1
        else:
            failures.append(f"STUCK count {s}: truth={gt_c}, detected={det_c}")

    # -- Check 3: 2-token correlation patterns --
    det_2tok = {}
    for r in stage_results:
        if len(r["pattern"]) == 2:
            det_2tok[tuple(r["pattern"])] = r["support_count"]

    gt_above = {k: v for k, v in gt_pairs.items() if v >= min_support}

    # All ground truth pairs with support >= min_support should be detected
    for pair, gt_c in gt_above.items():
        total += 1
        det_c = det_2tok.get(pair, 0)
        if gt_c == det_c:
            passed += 1
        else:
            failures.append(f"Pattern {pair}: truth={gt_c}, detected={det_c}")

    # No false positives
    for pair, det_c in det_2tok.items():
        gt_c = gt_pairs.get(pair, 0)
        total += 1
        if det_c == gt_c:
            passed += 1
        else:
            failures.append(f"False pos {pair}: detected={det_c}, truth={gt_c}")

    return passed, total, failures


# ===========================================================================
# MAIN
# ===========================================================================

def run_test(
    num_cases: int = NUM_CASES,
    delay_prob: float = DELAY_PROB,
    stuck_weights: List[int] = STUCK_WEIGHTS,
    min_support_frac: float = MIN_SUPPORT_FRAC,
    slow_multiplier: float = SLOW_MULTIPLIER,
    test_name: str = "unit_test",
) -> Tuple[int, int, List[str]]:
    """
    Run a full integration test. Returns (passed, total, failures).
    Can be called from other test files with different parameters.
    """
    print(f"\n{'='*60}")
    print(f"  {test_name}")
    print(f"  Cases={num_cases}, Delay={delay_prob}, Weights={stuck_weights}")
    print(f"{'='*60}")

    # 1. Compute ground truth
    print("Computing ground truth...")
    gt_slow, gt_stuck, gt_pairs = compute_ground_truth(num_cases, delay_prob, stuck_weights)

    # 2. Load into Neo4j
    print("Loading data into Neo4j...")
    load_data(num_cases, delay_prob, stuck_weights)

    # 3. Run logged pipeline
    log_dir = Path(__file__).parent / "logs" / test_name
    lp = LoggedPipeline(
        log_dir=log_dir,
        min_support_frac=min_support_frac,
        slow_multiplier=slow_multiplier,
    )
    load_env_auto()

    cases = lp.extract()
    canonical_order, transitions, blocking_point_map, drill_fields = lp.discover(cases)
    baselines, annotated = lp.baseline(cases, transitions, blocking_point_map, drill_fields)
    sequences, seq_cases = lp.tokenise(annotated, transitions)
    min_support = max(2, int(len(sequences) * min_support_frac))
    mined = lp.mine(sequences, min_support)
    stage_results, field_results = lp.filter_and_rank(
        mined, sequences, seq_cases, len(cases),
        transitions, drill_fields, min_support,
    )

    # 4. Verify
    print("Verifying...")
    passed, total, failures = verify(
        gt_slow, gt_stuck, gt_pairs, annotated, stage_results, min_support
    )

    # 5. Report
    pct = passed / total * 100 if total > 0 else 0
    print(f"\n  Result: {passed}/{total} checks passed ({pct:.0f}%)")
    if failures:
        print(f"  Failures:")
        for f in failures[:10]:
            print(f"    - {f}")
    else:
        print("  ALL CHECKS PASSED")
    print(f"  Logs: {log_dir}")

    return passed, total, failures


def main() -> None:
    passed, total, failures = run_test()
    sys.exit(0 if not failures else 1)


if __name__ == "__main__":
    main()
