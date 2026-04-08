"""
query.py -- Static extraction queries and database connection helpers.

This module houses the Neo4j Cypher queries and connection logic used by
the correlation finder engine. Swap out the query or connection details
here to point at a different knowledge graph without touching the engine.

The only requirement is that the graph follows the ProcessCase /
ProcessNode / ProcessEvent / ProcessEdge schema.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List

from dotenv import load_dotenv
from neo4j import GraphDatabase


# ==============================================================================
# ENV
# ==============================================================================

def load_env_auto() -> None:
    """Walk up from this file to find and load the nearest .env."""
    p = Path(__file__).resolve()
    for parent in [p.parent] + list(p.parents):
        env = parent / ".env"
        if env.exists():
            load_dotenv(env)
            return
    load_dotenv()


def req_env(name: str) -> str:
    """Return an env var or raise if missing."""
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def neo4j_driver():
    """Create and return a Neo4j driver from env vars."""
    return GraphDatabase.driver(
        req_env("NEO4J_URI"),
        auth=(req_env("NEO4J_USERNAME"), req_env("NEO4J_PASSWORD")),
    )


# ==============================================================================
# EXTRACTION QUERY
# ==============================================================================

EXTRACT_QUERY = """
MATCH (c:ProcessCase)
WITH c
MATCH (root:ProcessNode {case_id: c.case_id, node_type: 'CASE_ROOT'})
WITH c, root

OPTIONAL MATCH (ev:ProcessEvent {case_id: c.case_id})
WHERE ev.event_category = 'BUSINESS'
WITH c, root, ev ORDER BY ev.event_time
WITH c, root, collect(ev {
    .node_id, .event_type, .event_time, .actor_id, .to_state
}) AS events

RETURN
    c.case_id           AS case_id,
    c.status            AS case_status,
    c.last_event_time   AS last_event_time,
    root.attributes     AS root_attributes,
    events
ORDER BY c.case_id
"""


# ==============================================================================
# EXTRACTION FUNCTION
# ==============================================================================

def extract_cases(driver) -> List[Dict]:
    """Run the extraction query and return a list of case dicts."""
    with driver.session() as session:
        result = session.run(EXTRACT_QUERY)
        return [dict(r) for r in result]
