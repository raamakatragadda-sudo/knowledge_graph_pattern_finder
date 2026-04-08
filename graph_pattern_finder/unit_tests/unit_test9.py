"""
unit_test9.py -- Zero delay (0%), various stuck points.
Expected: no SLOW tokens at all, only STUCK tokens. No correlation patterns.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from graph_pattern_finder.unit_tests.unit_test import run_test

def main():
    p, t, f = run_test(
        num_cases=100,
        delay_prob=0.0,
        stuck_weights=[20, 20, 10, 15, 20, 15],
        test_name="unit_test9_zero_delay",
    )
    assert not f, f"FAILED: {len(f)} failures"
    print("unit_test9: PASSED")

if __name__ == "__main__":
    main()
