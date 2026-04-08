"""
unit_test4.py -- All cases stuck at PR_SUBMITTED (early bottleneck).
Expected: no delays (stuck at first stage), only STUCK:PR_SUBMITTED.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from graph_pattern_finder.unit_tests.unit_test import run_test

def main():
    p, t, f = run_test(
        num_cases=100,
        delay_prob=0.35,
        stuck_weights=[100, 0, 0, 0, 0, 0],
        test_name="unit_test4_all_stuck_early",
    )
    assert not f, f"FAILED: {len(f)} failures"
    print("unit_test4: PASSED")

if __name__ == "__main__":
    main()
