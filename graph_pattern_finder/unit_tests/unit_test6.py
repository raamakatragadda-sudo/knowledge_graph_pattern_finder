"""
unit_test6.py -- Even stuck distribution, moderate delay.
Expected: delays spread evenly, patterns at every stage.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from graph_pattern_finder.unit_tests.unit_test import run_test

def main():
    p, t, f = run_test(
        num_cases=100,
        delay_prob=0.35,
        stuck_weights=[16, 16, 17, 17, 17, 17],
        test_name="unit_test6_even_moderate",
    )
    assert not f, f"FAILED: {len(f)} failures"
    print("unit_test6: PASSED")

if __name__ == "__main__":
    main()
