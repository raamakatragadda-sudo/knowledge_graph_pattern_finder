"""
unit_test5.py -- All cases stuck at INVOICE_VALIDATED (late bottleneck).
Expected: delays can occur at all 5 earlier stages, rich pattern set.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from graph_pattern_finder.unit_tests.unit_test import run_test

def main():
    p, t, f = run_test(
        num_cases=100,
        delay_prob=0.35,
        stuck_weights=[0, 0, 0, 0, 0, 100],
        test_name="unit_test5_all_stuck_late",
    )
    assert not f, f"FAILED: {len(f)} failures"
    print("unit_test5: PASSED")

if __name__ == "__main__":
    main()
