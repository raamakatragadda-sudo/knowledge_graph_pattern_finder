"""
unit_test10.py -- Heavy middle: stuck at PO_ISSUED and GOODS_RECEIVED.
Expected: delays at PR and PO stages, stuck in the middle of pipeline.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from graph_pattern_finder.unit_tests.unit_test import run_test

def main():
    p, t, f = run_test(
        num_cases=100,
        delay_prob=0.45,
        stuck_weights=[0, 0, 50, 50, 0, 0],
        test_name="unit_test10_heavy_middle",
    )
    assert not f, f"FAILED: {len(f)} failures"
    print("unit_test10: PASSED")

if __name__ == "__main__":
    main()
