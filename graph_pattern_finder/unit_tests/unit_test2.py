"""
unit_test2.py -- Moderately high delay probability (40%).
Expected: more delays than baseline, richer multi-delay patterns.
Note: delay_prob must stay below ~45% so delayed cases remain the
minority and the 3x-median threshold can still detect them.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from graph_pattern_finder.unit_tests.unit_test import run_test

def main():
    p, t, f = run_test(
        num_cases=100,
        delay_prob=0.40,
        stuck_weights=[20, 20, 10, 15, 20, 15],
        test_name="unit_test2_high_delay",
    )
    assert not f, f"FAILED: {len(f)} failures"
    print("unit_test2: PASSED")

if __name__ == "__main__":
    main()
