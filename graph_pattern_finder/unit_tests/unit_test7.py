"""
unit_test7.py -- Bimodal: stuck only at PO_APPROVED or INVOICE_RECEIVED.
Expected: delays cluster around the two stuck points.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from graph_pattern_finder.unit_tests.unit_test import run_test

def main():
    p, t, f = run_test(
        num_cases=100,
        delay_prob=0.40,
        stuck_weights=[0, 50, 0, 0, 50, 0],
        test_name="unit_test7_bimodal",
    )
    assert not f, f"FAILED: {len(f)} failures"
    print("unit_test7: PASSED")

if __name__ == "__main__":
    main()
