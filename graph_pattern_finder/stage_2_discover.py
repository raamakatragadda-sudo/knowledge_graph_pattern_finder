"""
stage_2_discover.py -- Discover the pipeline structure from case data.

Observes all cases and automatically discovers:
  - The canonical ordering of event types in the pipeline
  - The transitions between consecutive events
  - The blocking point map (where cases get stuck)
  - The drill-down fields available in root node attributes

Standalone usage:
    python -m graph_pattern_finder.stage_2_discover

    Reads extracted_cases.json (from stage 1) and writes
    discovered_pipeline.json.

As a library:
    from graph_pattern_finder.stage_2_discover import discover_pipeline
    order, transitions, blocking_map, fields = discover_pipeline(cases)
"""

from __future__ import annotations

import json
from collections import defaultdict, Counter
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

from graph_pattern_finder.helpers import median


def discover_pipeline(cases: List[Dict]) -> Tuple[
    List[str],                          # canonical_event_order
    List[Tuple[str, str, str]],         # transitions: (step_name, from_event, to_event)
    Dict[str, Optional[str]],           # blocking_point_map
    List[str],                          # drill_fields
]:
    """
    Observe all cases and discover the pipeline structure.

    Returns a 4-tuple:
      canonical_order:    ordered list of event type names
      transitions:        list of (step_name, from_event, to_event) tuples
      blocking_point_map: dict mapping each event to its blocking step name
                          (None = completed/terminal)
      drill_fields:       list of categorical field names from root attributes
    """
    # -- Discover event order by observing median timeline position --
    event_positions: Dict[str, List[int]] = defaultdict(list)
    all_event_types: set = set()

    for case in cases:
        events = case.get("events") or []
        sorted_events = sorted(events, key=lambda e: e["event_time"] or "")
        for pos, ev in enumerate(sorted_events):
            etype = ev["event_type"]
            if etype:
                event_positions[etype].append(pos)
                all_event_types.add(etype)

    median_positions = {
        etype: median(positions)
        for etype, positions in event_positions.items()
    }
    canonical_order = sorted(
        all_event_types, key=lambda e: median_positions.get(e, 999)
    )

    # -- Discover transitions from consecutive event pairs --
    pair_counts: Counter = Counter()
    for case in cases:
        events = case.get("events") or []
        sorted_events = sorted(events, key=lambda e: e["event_time"] or "")
        etypes = [ev["event_type"] for ev in sorted_events if ev["event_type"]]
        for i in range(len(etypes) - 1):
            pair_counts[(etypes[i], etypes[i + 1])] += 1

    transitions: List[Tuple[str, str, str]] = []
    for i in range(len(canonical_order) - 1):
        ev_from = canonical_order[i]
        ev_to = canonical_order[i + 1]
        if pair_counts.get((ev_from, ev_to), 0) > 0:
            transitions.append((ev_from, ev_from, ev_to))

    # -- Build blocking point map --
    # Check if the terminal event has any OPEN cases ending there
    terminal_event = canonical_order[-1] if canonical_order else None
    terminal_has_open = False
    terminal_has_completed = False
    for case in cases:
        events = case.get("events") or []
        if not events:
            continue
        sorted_events = sorted(events, key=lambda e: e["event_time"] or "")
        last_evt = sorted_events[-1]["event_type"] if sorted_events else None
        if last_evt == terminal_event:
            if case["case_status"] == "OPEN":
                terminal_has_open = True
            else:
                terminal_has_completed = True

    blocking_point_map: Dict[str, Optional[str]] = {}
    for ev in canonical_order:
        if ev == terminal_event:
            # Terminal is a stuck point if any OPEN case ends here
            if terminal_has_open:
                blocking_point_map[ev] = ev
            else:
                blocking_point_map[ev] = None
        else:
            ev_idx = canonical_order.index(ev)
            found = False
            for t_step, t_from, t_to in transitions:
                t_from_idx = canonical_order.index(t_from)
                t_to_idx = canonical_order.index(t_to)
                if t_from_idx <= ev_idx < t_to_idx:
                    blocking_point_map[ev] = t_step
                    found = True
                    break
            if not found:
                blocking_point_map[ev] = transitions[-1][0] if transitions else ev

    # -- Discover drill-down fields from root attributes --
    field_counter: Counter = Counter()
    field_value_sets: Dict[str, set] = defaultdict(set)

    for case in cases:
        root_attrs = case.get("root_attributes") or "{}"
        if isinstance(root_attrs, str):
            try:
                root_attrs = json.loads(root_attrs)
            except (json.JSONDecodeError, TypeError):
                continue
        for key, val in root_attrs.items():
            if isinstance(val, str) and not key.startswith("scenario"):
                field_counter[key] += 1
                field_value_sets[key].add(val)

    n_cases = len(cases)
    drill_fields = sorted([
        field for field, count in field_counter.items()
        if count >= n_cases * 0.5
        and 1 < len(field_value_sets[field]) < n_cases
    ])

    return canonical_order, transitions, blocking_point_map, drill_fields


# -- Standalone entry point ---------------------------------------------------

def main() -> None:
    in_path = Path(__file__).parent / "extracted_cases.json"
    if not in_path.exists():
        print("Run stage_1_extract first to generate extracted_cases.json")
        return

    print("Stage 2: Discovering pipeline structure...")
    with open(in_path) as f:
        cases = json.load(f)

    canonical_order, transitions, blocking_point_map, drill_fields = discover_pipeline(cases)

    print(f"  Events:      {len(canonical_order)}")
    print(f"  Transitions: {len(transitions)}")
    print(f"  Fields:      {drill_fields}")

    out = {
        "canonical_event_order": canonical_order,
        "transitions": [
            {"step_name": s, "from_event": f, "to_event": t}
            for s, f, t in transitions
        ],
        "blocking_point_map": blocking_point_map,
        "drill_fields": drill_fields,
    }
    out_path = Path(__file__).parent / "discovered_pipeline.json"
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"  Written to {out_path}")


if __name__ == "__main__":
    main()
