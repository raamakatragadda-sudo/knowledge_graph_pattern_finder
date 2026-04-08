"""
stage_5_mine.py -- Mine frequent subsequences using PrefixSpan.

Takes token sequences (from stage 4 or any source) and runs PrefixSpan
to find all frequent subsequences above a minimum support threshold.

Standalone usage:
    python -m graph_pattern_finder.stage_5_mine

    Reads token_sequences.json, writes mined_patterns.json.

As a library:
    from graph_pattern_finder.stage_5_mine import mine_patterns
    results = mine_patterns(sequences, min_support=5)
    # returns [(support_count, [token, ...]), ...]
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Tuple

try:
    from prefixspan import PrefixSpan
except ImportError:
    print("ERROR: prefixspan not installed. Run: pip install prefixspan")
    raise SystemExit(1)


# Default minimum support fraction when running standalone
DEFAULT_MIN_SUPPORT_FRAC = 0.05


def mine_patterns(
    sequences: List[List[str]],
    min_support: int,
) -> List[Tuple[int, List[str]]]:
    """
    Run PrefixSpan on the given sequences and return all frequent
    subsequences with support >= min_support.

    Parameters
    ----------
    sequences:    list of token lists (each list is one case's token sequence)
    min_support:  absolute minimum number of sequences a pattern must appear in

    Returns
    -------
    List of (support_count, pattern) tuples, sorted by support descending.
    """
    ps = PrefixSpan(sequences)
    results = ps.frequent(min_support, closed=False)
    results.sort(key=lambda x: -x[0])
    return results


# -- Standalone entry point ---------------------------------------------------

def main() -> None:
    base = Path(__file__).parent
    tok_path = base / "token_sequences.json"

    if not tok_path.exists():
        print("Run stage 4 first to generate token_sequences.json")
        return

    print("Stage 5: Mining frequent subsequences with PrefixSpan...")
    with open(tok_path) as f:
        data = json.load(f)

    sequences = data["sequences"]
    min_support = max(2, int(len(sequences) * DEFAULT_MIN_SUPPORT_FRAC))
    print(f"  {len(sequences)} sequences, min_support={min_support}")

    results = mine_patterns(sequences, min_support)
    print(f"  Found {len(results)} frequent subsequences.")

    out = {
        "min_support": min_support,
        "total_sequences": len(sequences),
        "patterns": [
            {"support": sup, "pattern": pat}
            for sup, pat in results
        ],
    }
    out_path = base / "mined_patterns.json"
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"  Written to {out_path}")


if __name__ == "__main__":
    main()
