"""
stage_1_extract.py -- Extract cases from a Neo4j knowledge graph.

This is the data-loading stage. It connects to Neo4j, runs the
extraction query from query.py, and returns a list of case dicts.

Standalone usage:
    python -m graph_pattern_finder.stage_1_extract

    Extracts cases and writes them to extracted_cases.json so that
    later stages can consume them without a live database.

As a library:
    from graph_pattern_finder.stage_1_extract import extract
    cases = extract()  # returns List[Dict]
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Dict, List

# Add parent dir so we can import query.py from the project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from query import load_env_auto, neo4j_driver, extract_cases


def extract() -> List[Dict]:
    """
    Connect to Neo4j, run the extraction query, and return the raw
    case data as a list of dictionaries.

    Each dict contains:
      - case_id:         unique identifier for the case
      - case_status:     "OPEN" or "COMPLETED"
      - last_event_time: timestamp of the most recent event
      - root_attributes: JSON string of the root node's attributes
      - events:          list of business event dicts (sorted by time)
    """
    load_env_auto()
    driver = neo4j_driver()
    cases = extract_cases(driver)
    driver.close()
    return cases


# -- Standalone entry point ---------------------------------------------------

def main() -> None:
    print("Stage 1: Extracting cases from Neo4j...")
    cases = extract()
    print(f"  Extracted {len(cases)} cases.")

    out_path = Path(__file__).parent / "extracted_cases.json"
    with open(out_path, "w") as f:
        json.dump(cases, f, indent=2, default=str)
    print(f"  Written to {out_path}")


if __name__ == "__main__":
    main()
