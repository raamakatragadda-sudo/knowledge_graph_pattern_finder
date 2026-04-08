"""
unit_test3.py -- Low delay probability (10%).
Expected: very few delays, sparse patterns.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from graph_pattern_finder.unit_tests.unit_test import run_test

def main():
    p, t, f = run_test(
        num_cases=100,
        delay_prob=0.10,
        stuck_weights=[20, 20, 10, 15, 20, 15],
        test_name="unit_test3_low_delay",
    )
    assert not f, f"FAILED: {len(f)} failures"
    print("unit_test3: PASSED")

if __name__ == "__main__":
    main()
