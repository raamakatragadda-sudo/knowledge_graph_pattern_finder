"""
unit_test8.py -- Heavy late stuck with moderate delay (40%).
Expected: many cases reach INVOICE_VALIDATED, rich upstream delay patterns.
Note: delay_prob=1.0 makes delays the majority, so the median-based
detector can't distinguish them. Use 0.40 for detectable anomalies.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from graph_pattern_finder.unit_tests.unit_test import run_test

def main():
    p, t, f = run_test(
        num_cases=100,
        delay_prob=0.40,
        stuck_weights=[0, 0, 0, 0, 0, 100],
        test_name="unit_test8_heavy_late",
    )
    assert not f, f"FAILED: {len(f)} failures"
    print("unit_test8: PASSED")

if __name__ == "__main__":
    main()
