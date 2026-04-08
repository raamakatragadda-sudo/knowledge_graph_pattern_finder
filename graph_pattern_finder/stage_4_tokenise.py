"""
stage_4_tokenise.py -- Convert annotated cases into token sequences.

Takes the annotated cases from stage 3 and builds ordered token
sequences suitable for sequential pattern mining.

Each token is one of:
  SLOW:<step_name>             -- a transition that was abnormally slow
  STUCK:<blocking_point>       -- the case is stuck here (final token)

Optionally, tokens can be enriched with a field value:
  SLOW:<step_name>@<value>     -- e.g. SLOW:PR_SUBMITTED@COMP_1
  STUCK:<blocking_point>@<value>

Standalone usage:
    python -m graph_pattern_finder.stage_4_tokenise

    Reads annotated_cases.json and discovered_pipeline.json, writes
    token_sequences.json.

As a library:
    from graph_pattern_finder.stage_4_tokenise import tokenise_cases, tokenise_with_field
    sequences, matched_cases = tokenise_cases(annotated, transitions)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Any, Tuple


def tokenise_cases(
    annotated: List[Dict[str, Any]],
    transitions: List[Tuple[str, str, str]],
) -> Tuple[List[List[str]], List[Dict[str, Any]]]:
    """
    Build a token sequence for each annotated case.

    Tokens are emitted in pipeline order:
      1. SLOW:<step> for each slow transition (ordered by pipeline position)
      2. STUCK:<blocking_point> as the final token if the case is stuck

    Parameters
    ----------
    annotated:    list of annotated case dicts (from stage 3)
    transitions:  (step_name, from_event, to_event) tuples (from stage 2)

    Returns
    -------
    sequences:  list of token lists (one per case that has tokens)
    cases_out:  corresponding annotated case dicts (same length as sequences)
    """
    sequences: List[List[str]] = []
    cases_out: List[Dict[str, Any]] = []

    for ac in annotated:
        tokens: List[str] = []

        # SLOW tokens in pipeline order
        for step_name, _, _ in transitions:
            if step_name in ac["slow_steps"]:
                tokens.append(f"SLOW:{step_name}")

        # STUCK token at the end
        bp = ac["blocking_point"]
        if bp is not None:
            tokens.append(f"STUCK:{bp}")

        if tokens:
            sequences.append(tokens)
            cases_out.append(ac)

    return sequences, cases_out


def tokenise_with_field(
    annotated: List[Dict[str, Any]],
    transitions: List[Tuple[str, str, str]],
    field: str,
) -> Tuple[List[List[str]], List[Dict[str, Any]]]:
    """
    Same as tokenise_cases but each token is enriched with a field value.

    Example: SLOW:PR_SUBMITTED@COMP_1, STUCK:INVOICE_RECEIVED@COMP_1

    Parameters
    ----------
    annotated:    annotated case dicts
    transitions:  pipeline transitions
    field:        attribute field name to append (e.g. "company_code")
    """
    sequences: List[List[str]] = []
    cases_out: List[Dict[str, Any]] = []

    for ac in annotated:
        val = ac["attrs"].get(field, "UNKNOWN")
        tokens: List[str] = []

        for step_name, _, _ in transitions:
            if step_name in ac["slow_steps"]:
                tokens.append(f"SLOW:{step_name}@{val}")

        bp = ac["blocking_point"]
        if bp is not None:
            tokens.append(f"STUCK:{bp}@{val}")

        if tokens:
            sequences.append(tokens)
            cases_out.append(ac)

    return sequences, cases_out


# -- Standalone entry point ---------------------------------------------------

def main() -> None:
    base = Path(__file__).parent
    ann_path = base / "annotated_cases.json"
    pipe_path = base / "discovered_pipeline.json"

    if not ann_path.exists() or not pipe_path.exists():
        print("Run stages 2 and 3 first.")
        return

    print("Stage 4: Tokenising annotated cases...")
    with open(ann_path) as f:
        annotated = json.load(f)
    with open(pipe_path) as f:
        pipeline = json.load(f)

    transitions = [
        (t["step_name"], t["from_event"], t["to_event"])
        for t in pipeline["transitions"]
    ]

    sequences, cases_out = tokenise_cases(annotated, transitions)
    avg_len = sum(len(s) for s in sequences) / len(sequences) if sequences else 0

    print(f"  {len(sequences)} non-empty sequences, avg length {avg_len:.1f} tokens")

    out = {
        "sequences": sequences,
        "case_ids": [c["case_id"] for c in cases_out],
    }
    out_path = base / "token_sequences.json"
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"  Written to {out_path}")


if __name__ == "__main__":
    main()
