"""
scenario.py — The "OPEN (unfinished) cases" loader.

Unlike raw.py which creates COMPLETED shopping trips (every step done),
this file creates INCOMPLETE shopping trips that are STUCK somewhere
in the middle. Think of it like stories where the character got lost
halfway through — maybe they're waiting for approval, or the goods
never arrived, or the invoice is stuck. These "stuck" cases are useful
for testing how a system detects and handles problems.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

from dotenv import load_dotenv
from neo4j import GraphDatabase

# Make sure Python can find other files in the same folder as this script.
# It's like telling someone: "Hey, also check THIS folder when looking for things."
sys.path.insert(0, str(Path(__file__).resolve().parent))


# ============================================================
# ENV — Loading our secret settings
# ============================================================


def load_env_auto() -> None:
    """
    Finds and loads the secret settings file (.env).
    Think of .env as a locked diary with passwords and addresses.
    This function searches the current folder, then parent folders,
    going up the tree until it finds the diary. Once found, it reads
    everything so the program can use those secrets.
    """
    p = Path(__file__).resolve()
    for parent in [p.parent] + list(p.parents):
        env_path = parent / ".env"
        if env_path.exists():
            load_dotenv(dotenv_path=env_path)
            return
    load_dotenv()


# Run it immediately so secrets are ready before anything else runs.
load_env_auto()


def req_env(name: str) -> str:
    """
    Grabs a specific secret from the diary (.env file).
    If the secret is missing or blank, it throws an error
    because the program can't work without it.
    Think: "I NEED this ingredient — I can't bake the cake without it!"
    """
    v = (os.getenv(name) or "").strip()
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def neo4j_driver():
    """
    Creates a "phone line" (connection) to our Neo4j database.
    Neo4j is like a big whiteboard where we draw circles (nodes)
    and arrows (edges) to show how things are connected.
    We need the address, username, and password to connect.
    """
    return GraphDatabase.driver(
        req_env("NEO4J_URI"),
        auth=(req_env("NEO4J_USERNAME"), req_env("NEO4J_PASSWORD")),
    )


# ============================================================
# CONSTRAINTS — Rules to prevent duplicates
# ============================================================


def create_constraints(tx) -> None:
    """
    Tells the database: "Every item of these types MUST have a unique ID."
    It's like telling a school: "No two students can have the same student number."
    We set this rule for ProcessCase, ProcessNode, ProcessEdge, and ProcessEvent.
    This prevents accidentally creating duplicates.
    """
    queries = [
        """
        CREATE CONSTRAINT process_case_case_id IF NOT EXISTS
        FOR (n:ProcessCase)
        REQUIRE n.case_id IS UNIQUE
        """,
        """
        CREATE CONSTRAINT process_node_node_id IF NOT EXISTS
        FOR (n:ProcessNode)
        REQUIRE n.node_id IS UNIQUE
        """,
        """
        CREATE CONSTRAINT process_edge_edge_id IF NOT EXISTS
        FOR (n:ProcessEdge)
        REQUIRE n.edge_id IS UNIQUE
        """,
        """
        CREATE CONSTRAINT process_event_event_id IF NOT EXISTS
        FOR (n:ProcessEvent)
        REQUIRE n.event_id IS UNIQUE
        """,
    ]
    for q in queries:
        tx.run(q)


# ============================================================
# HELPERS — Small utility tools used by the bigger functions
# ============================================================


def z(dt: datetime | None) -> str | None:
    """
    Turns a date+time into a neat text string like "2025-01-15T08:00:00Z".
    The "Z" at the end means "this time is in UTC" (the world's shared clock).
    If you give it None (nothing), it politely returns None back.
    It also removes tiny microseconds because we don't need that much precision.
    """
    if dt is None:
        return None
    return (
        dt.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _offset_time(ts: Optional[str], **kwargs) -> Optional[str]:
    """
    Takes a time string and moves it forward (or backward) by some amount.
    Example: _offset_time("2025-01-15T08:00:00Z", days=30) → 30 days later.
    It's like saying "what time will it be 30 days from now?"
    If the input is empty or something goes wrong, it returns None.
    """
    if not ts:
        return None
    s = ts.rstrip("Z") + "+00:00"
    try:
        dt = datetime.fromisoformat(s) + timedelta(**kwargs)
        return z(dt)
    except Exception:
        return None


def make_node_id(case_id: str, *parts: str) -> str:
    """
    Makes a unique name tag for a node (circle on the whiteboard).
    Example: make_node_id("CASE_1", "PR") → "CASE_1::PR"
    The "::" is just a separator, like a hyphen in a phone number.
    """
    return "::".join([case_id, *parts])


def make_edge_id(
    case_id: str, rel_type: str, from_node_id: str, to_node_id: str
) -> str:
    """
    Makes a unique name tag for an edge (arrow on the whiteboard).
    It combines the case, what kind of arrow it is, and the two circles it connects.
    """
    return f"{case_id}::{rel_type}::{from_node_id}::{to_node_id}"


def make_event_id(case_id: str, node_id: str, event_type: str, event_time: str) -> str:
    """
    Makes a unique name tag for an event (something that happened).
    Combines case + node + event type + timestamp to be unique.
    """
    return f"{case_id}::{node_id}::{event_type}::{event_time}"


def chunked(seq: List[int], size: int):
    """
    Splits a big list into smaller chunks, like cutting a long sandwich
    into bite-sized pieces.
    Example: chunked([1,2,3,4,5], 2) → [1,2], [3,4], [5]
    """
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def build_common(idx: int) -> Dict[str, Any]:
    """
    Builds the SHARED information that every case needs, no matter
    which scenario (problem type) it has. Think of it as filling out
    the basic info on a form — name, address, amount, etc.

    Each case gets:
      - A unique case ID (like CASE_1, CASE_2, etc.)
      - IDs for each document (PR, PO, GR, Invoice, Payment)
      - People involved (requester, buyer, receiver, AP clerk, payer)
      - Business details (company, plant, supplier, material, amount)
      - Node IDs (unique name tags for each circle on the whiteboard)

    The modulo (%) math makes values rotate through a limited set,
    so we get realistic variety (e.g., only 2 companies, 5 plants,
    7 suppliers, 10 materials) rather than thousands of unique ones.
    """
    amount = float(1000 * idx)       # each case has a different dollar amount
    quantity = 10 * idx              # and a different quantity of items
    case_id = f"CASE_{idx}"

    return {
        "case_id": case_id,
        # Document IDs — like serial numbers for each piece of paperwork
        "pr_id": f"PR_{idx}",
        "po_id": f"PO_{idx}",
        "gr_id": f"GR_{idx}",
        "inv_id": f"INV_{idx}",
        "pay_id": f"PAY_{idx}",
        # People involved — each step has a different person responsible
        "req_actor": f"REQ_USER_{idx}",      # person who requested the purchase
        "buyer_actor": f"BUYER_{idx}",       # person who places the order
        "receiver_actor": f"RECEIVER_{idx}", # warehouse person who checks the delivery
        "ap_actor": f"AP_USER_{idx}",        # accounts payable person who checks the bill
        "pay_actor": f"PAY_USER_{idx}",      # person who sends the money
        # Business details — rotated so we get realistic variety
        "company_code": f"COMP_{((idx - 1) % 2) + 1}",   # alternates: COMP_1, COMP_2
        "plant_id": f"PLANT_{((idx - 1) % 5) + 1}",      # cycles through 5 plants
        "supplier_id": f"SUP_{((idx - 1) % 7) + 1}",     # cycles through 7 suppliers
        "material_id": f"MAT_{((idx - 1) % 10) + 1}",    # cycles through 10 materials
        "quantity": quantity,
        "amount": amount,
        "currency": "USD",
        "source_record_id": f"SRC_PR_{idx}",
        # Node IDs — unique name tags for circles on the whiteboard
        "root_node_id": make_node_id(case_id, "P2P"),       # the "book" itself
        "pr_node_id": make_node_id(case_id, "PR"),           # Purchase Request circle
        "po_node_id": make_node_id(case_id, "PO"),           # Purchase Order circle
        "gr_node_id": make_node_id(case_id, "GR"),           # Goods Receipt circle
        "inv_node_id": make_node_id(case_id, "INVOICE"),     # Invoice circle
        "pay_node_id": make_node_id(case_id, "PAYMENT"),     # Payment circle
    }


# ============================================================
# CLEANUP — Erase old data before writing new data
# ============================================================


def reset_case_graph_batch(tx, case_ids: List[str]) -> None:
    """
    Deletes ALL existing data for the given case IDs from the database.
    Think of it like erasing a whiteboard before drawing a new picture.
    We have to erase each type of thing separately:
      - ProcessCase (the case itself)
      - ProcessCaseSummary (any summary data)
      - ProcessNode (all the circles)
      - ProcessEdge (all the arrows)
      - ProcessEvent (all the diary entries)
    "DETACH DELETE" means "also remove any connections before deleting."
    This ensures we start fresh and don't mix old and new data.
    """
    if not case_ids:
        return

    tx.run(
        """
        MATCH (c:ProcessCase)
        WHERE c.case_id IN $case_ids
        DETACH DELETE c
        """,
        case_ids=case_ids,
    )
    tx.run(
        """
        MATCH (s:ProcessCaseSummary)
        WHERE s.case_id IN $case_ids
        DETACH DELETE s
        """,
        case_ids=case_ids,
    )
    tx.run(
        """
        MATCH (n:ProcessNode)
        WHERE n.case_id IN $case_ids
        DETACH DELETE n
        """,
        case_ids=case_ids,
    )
    tx.run(
        """
        MATCH (e:ProcessEdge)
        WHERE e.case_id IN $case_ids
        DETACH DELETE e
        """,
        case_ids=case_ids,
    )
    tx.run(
        """
        MATCH (ev:ProcessEvent)
        WHERE ev.case_id IN $case_ids
        DETACH DELETE ev
        """,
        case_ids=case_ids,
    )


# ============================================================
# OPEN SCENARIOS — The 8 types of "stuck" shopping trips
# ============================================================

# These are the 8 different ways a shopping trip can get stuck.
# Each one represents a real-world problem that happens in businesses:
#   1. PR_APPROVAL_DELAY       — Boss hasn't approved the request yet
#   2. PO_ISSUE_DELAY          — Order approved but not sent to the supplier yet
#   3. GR_DELAY                — Order sent but goods haven't arrived yet
#   4. INVOICE_NOT_RECEIVED    — Goods arrived but no bill from the supplier yet
#   5. INVOICE_VALIDATION_DELAY— Bill received but not checked/verified yet
#   6. PAYMENT_DELAY           — Bill verified but payment not sent yet
#   7. MATCH_FAILURE_BLOCK     — Bill doesn't match what we ordered (numbers don't add up!)
#   8. PRICE_VARIANCE_BLOCK    — The price on the order doesn't match what was agreed on

SCENARIOS: List[str] = [
    "PR_APPROVAL_DELAY",
    "PO_ISSUE_DELAY",
    "GR_DELAY",
    "INVOICE_NOT_RECEIVED",
    "INVOICE_VALIDATION_DELAY",
    "PAYMENT_DELAY",
    "MATCH_FAILURE_BLOCK",
    "PRICE_VARIANCE_BLOCK",
]


def build_scenario(idx: int, scenario_type: str) -> Dict[str, Any]:
    """
    Builds the timeline and details for ONE incomplete (stuck) case.

    Unlike raw.py where every step is completed, here we STOP partway through.
    Depending on the scenario_type, the story ends at a different chapter:

      - PR_APPROVAL_DELAY:        Story stops at "request submitted, waiting for boss"
      - PO_ISSUE_DELAY:           Story stops at "order approved but not sent to supplier"
      - GR_DELAY:                 Story stops at "order sent, waiting for goods to arrive"
      - INVOICE_NOT_RECEIVED:     Story stops at "goods arrived, waiting for the bill"
      - INVOICE_VALIDATION_DELAY: Story stops at "bill received, waiting to be checked"
      - PAYMENT_DELAY:            Story stops at "bill checked, waiting for payment"
      - MATCH_FAILURE_BLOCK:      Story stops because bill doesn't match the order
      - PRICE_VARIANCE_BLOCK:     Story stops because price is different than expected

    The function tracks WHERE the process is stuck (current_blocking_node_id)
    and WHAT state it's stuck in (current_blocking_state).
    """
    now = datetime.now(timezone.utc).replace(microsecond=0)
    base = build_common(idx)

    # Each case opened at a different time in the recent past (1-90 days ago).
    # The modulo math makes sure each case lands on a different date.
    opened_at = now - timedelta(days=1 + (idx * 13) % 89, hours=(idx * 5) % 12)

    # Every case at least has a PR that was created and submitted.
    pr_created = opened_at
    pr_submitted = pr_created + timedelta(hours=1 + (idx % 3))

    # Everything beyond PR_SUBMITTED starts as None (hasn't happened yet).
    # Each scenario will fill in only the steps that HAVE happened.
    pr_approved = None
    po_created = None
    po_approved = None
    po_issued = None
    gr_received = None
    inv_received = None
    inv_validated = None
    pay_paid = None

    # Status of each document — starts with only PR being SUBMITTED.
    pr_status = "SUBMITTED"
    po_status = None
    gr_status = None
    inv_status = None
    pay_status = None

    # --- Each scenario fills in how far the story got before getting stuck ---

    if scenario_type == "PR_APPROVAL_DELAY":
        # STUCK: The request was submitted, but nobody has approved it yet.
        # Like raising your hand in class and the teacher hasn't called on you.
        last_event_time = pr_submitted
        current_blocking_node_id = base["pr_node_id"]
        current_blocking_state = "PR_SUBMITTED"

    elif scenario_type == "PO_ISSUE_DELAY":
        # STUCK: PR was approved, PO was created and approved, but hasn't been
        # sent to the supplier yet. Like writing a letter but not mailing it.
        pr_approved = pr_submitted + timedelta(hours=3 + (idx % 5))
        po_created = pr_approved + timedelta(hours=4)
        po_approved = po_created + timedelta(hours=5 + (idx % 4))
        pr_status = "APPROVED"
        po_status = "APPROVED"
        last_event_time = po_approved
        current_blocking_node_id = base["po_node_id"]
        current_blocking_state = "PO_APPROVED"

    elif scenario_type == "GR_DELAY":
        # STUCK: The order was sent to the supplier, but goods haven't arrived.
        # Like ordering a pizza and waiting by the door.
        pr_approved = pr_submitted + timedelta(hours=3 + (idx % 4))
        po_created = pr_approved + timedelta(hours=3)
        po_approved = po_created + timedelta(hours=2 + (idx % 3))
        po_issued = po_approved + timedelta(hours=2 + (idx % 3))
        pr_status = "APPROVED"
        po_status = "ISSUED"
        last_event_time = po_issued
        current_blocking_node_id = base["po_node_id"]
        current_blocking_state = "PO_ISSUED"

    elif scenario_type == "INVOICE_NOT_RECEIVED":
        # STUCK: Goods arrived but the supplier hasn't sent a bill yet.
        # Like getting your food at a restaurant but the waiter forgot the check.
        pr_approved = pr_submitted + timedelta(hours=3 + (idx % 4))
        po_created = pr_approved + timedelta(hours=3)
        po_approved = po_created + timedelta(hours=2 + (idx % 3))
        po_issued = po_approved + timedelta(hours=2)
        gr_received = po_issued + timedelta(days=3 + (idx % 4))
        pr_status = "APPROVED"
        po_status = "ISSUED"
        gr_status = "RECEIVED"
        last_event_time = gr_received
        current_blocking_node_id = base["gr_node_id"]
        current_blocking_state = "GOODS_RECEIVED"

    elif scenario_type == "INVOICE_VALIDATION_DELAY":
        # STUCK: The bill came in but nobody has checked if it's correct.
        # Like getting a test back but the teacher hasn't graded it.
        pr_approved = pr_submitted + timedelta(hours=3 + (idx % 4))
        po_created = pr_approved + timedelta(hours=3)
        po_approved = po_created + timedelta(hours=2 + (idx % 3))
        po_issued = po_approved + timedelta(hours=2)
        gr_received = po_issued + timedelta(days=3 + (idx % 3))
        inv_received = gr_received + timedelta(days=1, hours=(idx % 5))
        pr_status = "APPROVED"
        po_status = "ISSUED"
        gr_status = "RECEIVED"
        inv_status = "RECEIVED"
        last_event_time = inv_received
        current_blocking_node_id = base["inv_node_id"]
        current_blocking_state = "INVOICE_RECEIVED"

    elif scenario_type == "PAYMENT_DELAY":
        # STUCK: Bill was checked and is correct, but payment hasn't been sent.
        # Like knowing you owe someone money but not having paid them yet.
        pr_approved = pr_submitted + timedelta(hours=3 + (idx % 4))
        po_created = pr_approved + timedelta(hours=3)
        po_approved = po_created + timedelta(hours=2 + (idx % 3))
        po_issued = po_approved + timedelta(hours=2)
        gr_received = po_issued + timedelta(days=3 + (idx % 3))
        inv_received = gr_received + timedelta(days=1, hours=(idx % 4))
        inv_validated = inv_received + timedelta(hours=4 + (idx % 4))
        pr_status = "APPROVED"
        po_status = "ISSUED"
        gr_status = "RECEIVED"
        inv_status = "VALIDATED"
        last_event_time = inv_validated
        current_blocking_node_id = base["inv_node_id"]
        current_blocking_state = "INVOICE_VALIDATED"

    elif scenario_type == "MATCH_FAILURE_BLOCK":
        # STUCK: The bill doesn't match the order — the numbers don't add up!
        # Like ordering 10 apples but the bill says 12. Someone needs to fix this.
        pr_approved = pr_submitted + timedelta(hours=3 + (idx % 4))
        po_created = pr_approved + timedelta(hours=3)
        po_approved = po_created + timedelta(hours=2 + (idx % 3))
        po_issued = po_approved + timedelta(hours=2)
        gr_received = po_issued + timedelta(days=3 + (idx % 3))
        inv_received = gr_received + timedelta(days=1, hours=(idx % 5))
        pr_status = "APPROVED"
        po_status = "ISSUED"
        gr_status = "RECEIVED"
        inv_status = "RECEIVED"
        last_event_time = inv_received
        current_blocking_node_id = base["inv_node_id"]
        current_blocking_state = "INVOICE_RECEIVED"

    elif scenario_type == "PRICE_VARIANCE_BLOCK":
        # STUCK: The price on the PO doesn't match the agreed-upon price.
        # Like a store charging you $5 for candy that was labeled $3.
        pr_approved = pr_submitted + timedelta(hours=3 + (idx % 4))
        po_created = pr_approved + timedelta(hours=3)
        pr_status = "APPROVED"
        po_status = "CREATED"
        last_event_time = po_created
        current_blocking_node_id = base["po_node_id"]
        current_blocking_state = "PO_CREATED"

    else:
        raise ValueError(f"Unknown scenario_type: {scenario_type}")

    # --- Payment terms: how long the supplier gives us to pay ---
    # Cycles through 4 options (like rotating through 4 flavors of ice cream)
    _pt_cycle = ["NET30", "NET45", "ZB30", "2-10NET30"]
    payment_terms = _pt_cycle[idx % 4]
    net_days = {"NET30": 30, "NET45": 45, "ZB30": 30, "2-10NET30": 30}[payment_terms]
    discount_pct = 2.0 if payment_terms == "2-10NET30" else 0.0  # 2% discount if paid early

    # --- Quantity handling ---
    ordered_qty = base["quantity"]
    # Every 5th case: we only received 80% of what we ordered (a short delivery!)
    received_qty = ordered_qty if idx % 5 != 0 else int(ordered_qty * 0.8)
    invoice_qty = received_qty

    # Combine the base info with the scenario-specific info into one big dictionary.
    # Convert all datetime objects to ISO text strings using z().
    p = dict(base)
    p.update(
        {
            "scenario_type": scenario_type,
            "payment_terms": payment_terms,
            "net_days": net_days,
            "discount_pct": discount_pct,
            "ordered_qty": ordered_qty,
            "received_qty": received_qty,
            "invoice_qty": invoice_qty,
            "opened_at": z(opened_at),
            "last_event_time": z(last_event_time),
            "closed_at": None,                     # None = this case is NOT finished!
            "pr_created": z(pr_created),
            "pr_submitted": z(pr_submitted),
            "pr_approved": z(pr_approved),          # None if PR not yet approved
            "pr_status": pr_status,
            "po_created": z(po_created),            # None if PO not yet created
            "po_approved": z(po_approved),
            "po_issued": z(po_issued),
            "po_status": po_status,
            "gr_received": z(gr_received),          # None if goods not yet received
            "gr_status": gr_status,
            "inv_received": z(inv_received),        # None if invoice not yet received
            "inv_validated": z(inv_validated),
            "inv_status": inv_status,
            "pay_paid": z(pay_paid),                # Always None — no open case has been paid!
            "pay_status": pay_status,
            "current_blocking_node_id": current_blocking_node_id,  # WHERE is it stuck?
            "current_blocking_state": current_blocking_state,      # WHAT state is it stuck in?
        }
    )
    return p


# ============================================================
# BATCH UPSERTS — Saving data to the database
# ============================================================
# "Upsert" means: "If this thing already exists, update it. If not, create it."
# Think of it like a sticker book — if you already have the sticker, you replace
# it with a newer version. If you don't have it, you stick a new one in.
# "Batch" means we do many at once instead of one at a time (much faster!).


def upsert_process_cases_batch(tx, rows: List[Dict[str, Any]]) -> None:
    """
    Saves a bunch of "cases" to the database at once.
    A case is like one shopping trip story. Here, all cases are OPEN (status = "OPEN")
    and process_type is always "P2P" (Procure-to-Pay), because these are
    all stuck/incomplete procurement processes.
    """
    if not rows:
        return
    tx.run(
        """
        UNWIND $rows AS row
        MERGE (c:ProcessCase {case_id: row.case_id})
        SET
          c.process_type = "P2P",
          c.root_node_id = row.root_node_id,
          c.status = "OPEN",
          c.opened_at = row.opened_at,
          c.closed_at = row.closed_at,
          c.last_event_time = row.last_event_time,
          c.attributes = row.attributes
        """,
        rows=rows,
    )


def upsert_process_nodes_batch(tx, rows: List[Dict[str, Any]]) -> None:
    """
    Saves a bunch of "nodes" (circles) to the database at once.
    Each node represents a step in the shopping trip — PR, PO, GR, Invoice, Payment.
    Some nodes might be missing (because the case got stuck before reaching them).
    """
    if not rows:
        return
    tx.run(
        """
        UNWIND $rows AS row
        MERGE (n:ProcessNode {node_id: row.node_id})
        SET
          n.case_id = row.case_id,
          n.parent_node_id = row.parent_node_id,
          n.node_type = row.node_type,
          n.node_subtype = row.node_subtype,
          n.entity_id = row.entity_id,
          n.entity_type = row.entity_type,
          n.level_num = row.level_num,
          n.status = row.status,
          n.state_code = row.state_code,
          n.start_time = row.start_time,
          n.end_time = row.end_time,
          n.source_system = row.source_system,
          n.owner_actor = row.owner_actor,
          n.attributes = row.attributes
        """,
        rows=rows,
    )


def upsert_process_edges_batch(tx, rows: List[Dict[str, Any]]) -> None:
    """
    Saves a bunch of "edges" (arrows) to the database at once.
    Edges show how steps connect — like "PO was GENERATED FROM PR" or
    "Invoice MATCHED TO PO". Only edges for steps that actually happened are saved.
    """
    if not rows:
        return
    tx.run(
        """
        UNWIND $rows AS row
        MERGE (e:ProcessEdge {edge_id: row.edge_id})
        SET
          e.case_id = row.case_id,
          e.from_node_id = row.from_node_id,
          e.to_node_id = row.to_node_id,
          e.relationship_type = row.relationship_type,
          e.effective_from = row.effective_from,
          e.effective_to = row.effective_to,
          e.attributes = row.attributes
        """,
        rows=rows,
    )


def upsert_process_events_batch(tx, rows: List[Dict[str, Any]]) -> None:
    """
    Saves a bunch of "events" (diary entries) to the database at once.
    Events record WHAT happened, WHEN, and WHO did it.
    Like: "PR_CREATED at 8am by REQ_USER_1" or "MATCH_FAILED at 3pm by AP_USER_5"
    """
    if not rows:
        return
    tx.run(
        """
        UNWIND $rows AS row
        MERGE (e:ProcessEvent {event_id: row.event_id})
        SET
          e.case_id = row.case_id,
          e.node_id = row.node_id,
          e.event_type = row.event_type,
          e.event_category = row.event_category,
          e.event_time = row.event_time,
          e.actor_id = row.actor_id,
          e.from_state = row.from_state,
          e.to_state = row.to_state,
          e.payload = row.payload
        """,
        rows=rows,
    )


# ============================================================
# BUILD NODE / EDGE / EVENT PAYLOADS
# ============================================================
# These functions build the actual data (nodes, edges, events) for each case.
# The key difference from raw.py: here, nodes/edges/events are only created
# for steps that have ACTUALLY HAPPENED. If a case is stuck at step 3,
# steps 4 and 5 simply don't exist yet.


def build_nodes(p: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Builds the list of nodes (circles on the whiteboard) for ONE case.

    Always creates:
      - ROOT node (the "book cover" for the whole case)
      - PR node (Purchase Request — always exists since every case starts here)

    Conditionally creates (only if that step has happened):
      - PO node (if po_created is not None)
      - GR node (if gr_received is not None)
      - Invoice node (if inv_received is not None)
      - Payment node (if pay_paid is not None — always None for open cases!)

    Uses a helper function add_node() to avoid repeating the same
    dictionary structure over and over. It's like having a form template.
    """
    nodes: List[Dict[str, Any]] = []

    def add_node(
        node_id: str,
        parent_node_id: Optional[str],
        node_type: str,
        node_subtype: str,
        entity_id: Optional[str],
        entity_type: Optional[str],
        level_num: int,
        status: Optional[str],
        state_code: Optional[str],
        start_time: Optional[str],
        end_time: Optional[str],
        owner_actor: Optional[str],
        attrs: Dict[str, Any],
    ) -> None:
        """Inner helper: fills in a node dictionary and adds it to the list."""
        nodes.append(
            {
                "node_id": node_id,
                "case_id": p["case_id"],
                "parent_node_id": parent_node_id,
                "node_type": node_type,
                "node_subtype": node_subtype,
                "entity_id": entity_id,
                "entity_type": entity_type,
                "level_num": level_num,
                "status": status,
                "state_code": state_code,
                "start_time": start_time,
                "end_time": end_time,
                "source_system": "synthetic_open_loader",  # marks this as generated data
                "owner_actor": owner_actor,
                "attributes": json.dumps(attrs),           # extra info stored as JSON text
            }
        )

    # --- ROOT node: the "book" itself that contains all chapters ---
    add_node(
        node_id=p["root_node_id"],
        parent_node_id=None,
        node_type="CASE_ROOT",
        node_subtype="P2P",
        entity_id=None,
        entity_type=None,
        level_num=0,
        status="OPEN",
        state_code=p["current_blocking_state"],
        start_time=p["opened_at"],
        end_time=None,
        owner_actor=None,
        attrs={
            "scenario_type": p["scenario_type"],
            "company_code": p["company_code"],
            "supplier_id": p["supplier_id"],
            "plant_id": p["plant_id"],
            "material_id": p["material_id"],
            "quantity": p["quantity"],
            "amount": p["amount"],
            "currency": p["currency"],
        },
    )

    # --- PR node: "Can I buy this?" (always exists) ---
    add_node(
        node_id=p["pr_node_id"],
        parent_node_id=p["root_node_id"],
        node_type="BUSINESS_OBJECT",
        node_subtype="REQUISITION",
        entity_id=p["pr_id"],
        entity_type="PR",
        level_num=1,
        status=p["pr_status"],
        state_code="PR_APPROVED" if p["pr_approved"] else "PR_SUBMITTED",
        start_time=p["pr_created"],
        end_time=p["pr_approved"],
        owner_actor=p["req_actor"],
        attrs={
            "entity_line_id": "1",
            "company_code": p["company_code"],
            "plant_id": p["plant_id"],
            "supplier_id": p["supplier_id"],
            "material_id": p["material_id"],
            "quantity": p["quantity"],
            "amount": p["amount"],
            "currency": p["currency"],
            "source_record_id": p["source_record_id"],
        },
    )

    # --- PO node: "Let's officially order it" (only if PO was created) ---
    if p["po_created"]:
        add_node(
            node_id=p["po_node_id"],
            parent_node_id=p["root_node_id"],
            node_type="BUSINESS_OBJECT",
            node_subtype="PURCHASE_ORDER",
            entity_id=p["po_id"],
            entity_type="PO",
            level_num=1,
            status=p["po_status"],
            state_code="PO_ISSUED"
            if p["po_issued"]
            else "PO_APPROVED"
            if p["po_approved"]
            else "PO_CREATED",
            start_time=p["po_created"],
            end_time=p["po_issued"] or p["po_approved"],
            owner_actor=p["buyer_actor"],
            attrs={
                "entity_line_id": "1",
                "parent_entity_id": p["pr_id"],
                "parent_entity_type": "PR",
                "company_code": p["company_code"],
                "plant_id": p["plant_id"],
                "supplier_id": p["supplier_id"],
                "material_id": p["material_id"],
                "quantity": p["quantity"],
                "amount": p["amount"],
                "currency": p["currency"],
                "payment_terms": p.get("payment_terms"),
                "net_days": p.get("net_days"),
                "discount_pct": p.get("discount_pct"),
            },
        )

    # --- GR node: "The stuff arrived!" (only if goods were received) ---
    if p["gr_received"]:
        add_node(
            node_id=p["gr_node_id"],
            parent_node_id=p["root_node_id"],
            node_type="BUSINESS_OBJECT",
            node_subtype="GOODS_RECEIPT",
            entity_id=p["gr_id"],
            entity_type="GR",
            level_num=1,
            status=p["gr_status"],
            state_code="GOODS_RECEIVED",
            start_time=p["gr_received"],
            end_time=p["gr_received"],
            owner_actor=p["receiver_actor"],
            attrs={
                "entity_line_id": "1",
                "parent_entity_id": p["po_id"],
                "parent_entity_type": "PO",
                "plant_id": p["plant_id"],
                "supplier_id": p["supplier_id"],
                "material_id": p["material_id"],
                "ordered_qty": p.get("ordered_qty", p["quantity"]),
                "received_qty": p.get("received_qty", p["quantity"]),
            },
        )

    # --- Invoice node: "Here's the bill" (only if invoice was received) ---
    if p["inv_received"]:
        add_node(
            node_id=p["inv_node_id"],
            parent_node_id=p["root_node_id"],
            node_type="BUSINESS_OBJECT",
            node_subtype="INVOICE",
            entity_id=p["inv_id"],
            entity_type="Invoice",
            level_num=1,
            status=p["inv_status"],
            state_code="INVOICE_VALIDATED"
            if p["inv_validated"]
            else "INVOICE_RECEIVED",
            start_time=p["inv_received"],
            end_time=p["inv_validated"],
            owner_actor=p["ap_actor"],
            attrs={
                "entity_line_id": "1",
                "parent_entity_id": p["po_id"],
                "secondary_parent_entity_id": p["gr_id"] if p["gr_received"] else None,
                "supplier_id": p["supplier_id"],
                "invoice_qty": p.get("invoice_qty"),
                "invoice_amount": round(
                    float(p["amount"])
                    * (p.get("received_qty") or p["quantity"])
                    / p["quantity"],
                    2,
                ),
                "amount": p["amount"],
                "currency": p["currency"],
                "payment_due_date": _offset_time(
                    p["inv_received"], days=p.get("net_days", 30)
                ),
                "discount_date": _offset_time(p["inv_received"], days=10)
                if p.get("payment_terms") == "2-10NET30"
                else None,
            },
        )

    # --- Payment node: "We paid!" (only if payment happened — never for open cases) ---
    if p["pay_paid"]:
        add_node(
            node_id=p["pay_node_id"],
            parent_node_id=p["root_node_id"],
            node_type="BUSINESS_OBJECT",
            node_subtype="PAYMENT",
            entity_id=p["pay_id"],
            entity_type="Payment",
            level_num=1,
            status=p["pay_status"],
            state_code="PAYMENT_COMPLETED",
            start_time=p["pay_paid"],
            end_time=p["pay_paid"],
            owner_actor=p["pay_actor"],
            attrs={
                "parent_entity_id": p["inv_id"],
                "parent_entity_type": "Invoice",
                "supplier_id": p["supplier_id"],
                "amount": p["amount"],
                "currency": p["currency"],
            },
        )

    return nodes


def build_edges(p: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Builds the list of edges (arrows on the whiteboard) for ONE case.

    Edges show how steps are connected. There are two kinds:
      1. "CONTAINS" arrows: Root → each step (like a table of contents)
      2. Relationship arrows: show the flow of work:
         - PO was GENERATED_FROM PR (you can't order without asking first!)
         - GR FOLLOWS PO (stuff arrives after you order it)
         - Invoice MATCHED_TO PO and GR (checking the bill against what we ordered/received)
         - Payment SETTLES Invoice (paying the bill = done!)

    Only edges for steps that actually happened are created.
    Uses add_edge() helper to avoid repeating dictionary structure.
    """
    edges: List[Dict[str, Any]] = []

    def add_edge(
        rel_type: str,
        from_node_id: str,
        to_node_id: str,
        effective_from: Optional[str],
        attrs: Dict[str, Any],
    ) -> None:
        """Inner helper: fills in an edge dictionary and adds it to the list."""
        edges.append(
            {
                "edge_id": make_edge_id(
                    p["case_id"], rel_type, from_node_id, to_node_id
                ),
                "case_id": p["case_id"],
                "from_node_id": from_node_id,
                "to_node_id": to_node_id,
                "relationship_type": rel_type,
                "effective_from": effective_from,
                "effective_to": None,
                "attributes": json.dumps(attrs),
            }
        )

    # Root always CONTAINS the PR (every case has at least a Purchase Request)
    add_edge("CONTAINS", p["root_node_id"], p["pr_node_id"], p["pr_created"], {})

    # If a Purchase Order was created...
    if p["po_created"]:
        add_edge("CONTAINS", p["root_node_id"], p["po_node_id"], p["po_created"], {})
        # "The PO was GENERATED FROM the PR" (you ordered because someone asked)
        add_edge(
            "GENERATED_FROM", p["po_node_id"], p["pr_node_id"], p["po_created"], {}
        )

    # If goods were received...
    if p["gr_received"]:
        add_edge("CONTAINS", p["root_node_id"], p["gr_node_id"], p["gr_received"], {})
        # "Goods Receipt FOLLOWS the Purchase Order" (stuff arrives after ordering)
        add_edge("FOLLOWS", p["gr_node_id"], p["po_node_id"], p["gr_received"], {})

    # If an invoice was received...
    if p["inv_received"]:
        add_edge("CONTAINS", p["root_node_id"], p["inv_node_id"], p["inv_received"], {})
        # "Invoice MATCHED TO the PO" (does the bill match what we ordered?)
        add_edge(
            "MATCHED_TO",
            p["inv_node_id"],
            p["po_node_id"],
            p["inv_received"],
            {"match_basis": "PO"},
        )
        # Also match to GR if goods were received (does bill match what arrived?)
        if p["gr_received"]:
            add_edge(
                "MATCHED_TO",
                p["inv_node_id"],
                p["gr_node_id"],
                p["inv_received"],
                {"match_basis": "GR"},
            )

    # If payment was made (never happens for open cases, but the code handles it)
    if p["pay_paid"]:
        add_edge("CONTAINS", p["root_node_id"], p["pay_node_id"], p["pay_paid"], {})
        # "Payment SETTLES the Invoice" (we paid the bill!)
        add_edge("SETTLES", p["pay_node_id"], p["inv_node_id"], p["pay_paid"], {})

    return edges


def build_events(p: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Builds the list of events (diary entries) for ONE case.

    Events are like a log book: "WHAT happened, WHEN, and WHO did it."
    The clever trick here: add_event() silently skips events where
    event_time is None. So we can call it for ALL possible events,
    and only the ones that actually happened will be recorded.

    For example, if a case is stuck at PR_SUBMITTED, then pr_approved
    is None, so the PR_APPROVED event is automatically skipped.

    Two special scenarios also add EXCEPTION events:
      - MATCH_FAILURE_BLOCK: adds a "MATCH_FAILED" event (bill didn't match)
      - PRICE_VARIANCE_BLOCK: adds a "PRICE_VARIANCE_FLAGGED" event (wrong price)
    """
    events: List[Dict[str, Any]] = []

    def add_event(
        node_id: str,
        event_type: str,
        event_time: Optional[str],
        actor_id: Optional[str],
        to_state: Optional[str],
        event_category: str = "BUSINESS",
    ) -> None:
        """Inner helper: adds an event to the list, but ONLY if event_time exists."""
        if not event_time:
            return  # Skip — this step hasn't happened yet!
        events.append(
            {
                "event_id": make_event_id(
                    p["case_id"], node_id, event_type, event_time
                ),
                "case_id": p["case_id"],
                "node_id": node_id,
                "event_type": event_type,
                "event_category": event_category,
                "event_time": event_time,
                "actor_id": actor_id,
                "from_state": None,
                "to_state": to_state,
                "payload": json.dumps({}),
            }
        )

    # --- PR events: the life of a Purchase Request ---
    add_event(p["pr_node_id"], "PR_CREATED", p["pr_created"], p["req_actor"], "DRAFT")
    add_event(
        p["pr_node_id"], "PR_SUBMITTED", p["pr_submitted"], p["req_actor"], "SUBMITTED"
    )
    add_event(
        p["pr_node_id"], "PR_APPROVED", p["pr_approved"], p["req_actor"], "APPROVED"
    )

    # --- PO events: the life of a Purchase Order ---
    add_event(
        p["po_node_id"], "PO_CREATED", p["po_created"], p["buyer_actor"], "CREATED"
    )
    add_event(
        p["po_node_id"], "PO_APPROVED", p["po_approved"], p["buyer_actor"], "APPROVED"
    )
    add_event(p["po_node_id"], "PO_ISSUED", p["po_issued"], p["buyer_actor"], "ISSUED")

    # --- GR event: goods arrived ---
    add_event(
        p["gr_node_id"],
        "GOODS_RECEIVED",
        p["gr_received"],
        p["receiver_actor"],
        "RECEIVED",
    )

    # --- Invoice events ---
    add_event(
        p["inv_node_id"],
        "INVOICE_RECEIVED",
        p["inv_received"],
        p["ap_actor"],
        "RECEIVED",
    )
    add_event(
        p["inv_node_id"],
        "INVOICE_VALIDATED",
        p["inv_validated"],
        p["ap_actor"],
        "VALIDATED",
    )

    # --- Payment event: money sent! ---
    add_event(
        p["pay_node_id"], "PAYMENT_COMPLETED", p["pay_paid"], p["pay_actor"], "PAID"
    )

    # --- EXCEPTION events: something went wrong! ---

    # MATCH_FAILURE_BLOCK: 45 minutes after invoice arrived, the system found
    # that the bill doesn't match what was ordered. Like finding a typo on a test.
    if p["scenario_type"] == "MATCH_FAILURE_BLOCK" and p.get("inv_received"):
        add_event(
            p["inv_node_id"],
            "MATCH_FAILED",
            _offset_time(p["inv_received"], minutes=45),
            p["ap_actor"],
            "MATCH_FAILED",
            "EXCEPTION",
        )

    # PRICE_VARIANCE_BLOCK: 30 minutes after PO was created, the system found
    # the price is different than expected. Like a store charging the wrong price.
    if p["scenario_type"] == "PRICE_VARIANCE_BLOCK" and p.get("po_created"):
        add_event(
            p["po_node_id"],
            "PRICE_VARIANCE_FLAGGED",
            _offset_time(p["po_created"], minutes=30),
            p["buyer_actor"],
            "PRICE_VARIANCE",
            "EXCEPTION",
        )

    return events


# ============================================================
# BATCH BUILD — Putting it all together
# ============================================================


def build_case_payload(idx: int) -> Dict[str, Any]:
    """
    Builds ALL the data for one OPEN (stuck) case.

    It picks a scenario by cycling through the 8 scenario types:
      Case 1 → PR_APPROVAL_DELAY
      Case 2 → PO_ISSUE_DELAY
      Case 3 → GR_DELAY
      ...and so on, wrapping around after 8.

    Then it builds the scenario data (timeline, statuses, etc.),
    creates the case "book cover", and builds all the nodes (circles),
    edges (arrows), and events (diary entries) for that case.

    Returns everything bundled together as one dictionary — ready to be
    saved to the database.
    """
    # Pick which scenario type this case will be (cycles through all 8)
    scenario_type = SCENARIOS[(idx - 1) % len(SCENARIOS)]
    p = build_scenario(idx, scenario_type)

    # The "book cover" — summary info about this case
    process_case = {
        "case_id": p["case_id"],
        "root_node_id": p["root_node_id"],
        "opened_at": p["opened_at"],
        "closed_at": p["closed_at"],           # always None for open cases
        "last_event_time": p["last_event_time"],
        "attributes": json.dumps(
            {
                "scenario_type": p["scenario_type"],
                "company_code": p["company_code"],
                "supplier_id": p["supplier_id"],
                "plant_id": p["plant_id"],
                "material_id": p["material_id"],
                "currency": p["currency"],
            }
        ),
    }

    # Build all the pieces of the case
    nodes = build_nodes(p)     # circles on the whiteboard
    edges = build_edges(p)     # arrows connecting the circles
    events = build_events(p)   # diary entries of what happened

    return {
        "case_id": p["case_id"],
        "process_case": process_case,
        "nodes": nodes,
        "edges": edges,
        "events": events,
    }


def upsert_open_case_batch(tx, payloads: List[Dict[str, Any]]) -> None:
    """
    Takes a batch of complete case payloads and saves them ALL to the database.

    First, it ERASES any existing data for these case IDs (clean slate).
    Then it collects all cases, nodes, edges, and events into big lists
    and saves each list in one go — like mailing one big package instead
    of many small letters. Much faster!
    """
    # Step 1: Erase old data for these cases
    case_ids = [p["case_id"] for p in payloads]
    reset_case_graph_batch(tx, case_ids)

    # Step 2: Collect all pieces into big lists
    all_cases: List[Dict[str, Any]] = []
    all_nodes: List[Dict[str, Any]] = []
    all_edges: List[Dict[str, Any]] = []
    all_events: List[Dict[str, Any]] = []

    for payload in payloads:
        all_cases.append(payload["process_case"])
        all_nodes.extend(payload["nodes"])
        all_edges.extend(payload["edges"])
        all_events.extend(payload["events"])

    # Step 3: Save everything to the database in bulk
    upsert_process_cases_batch(tx, all_cases)
    upsert_process_nodes_batch(tx, all_nodes)
    upsert_process_edges_batch(tx, all_edges)
    upsert_process_events_batch(tx, all_events)


# ============================================================
# TRUTH FILE — Record exactly what exceptions/patterns we planted
# ============================================================


def generate_truth_file(num_instances: int) -> str:
    """
    Builds a JSON "answer key" of every exception and blocking pattern
    that scenario.py planted into the database.

    This is the TRUTH — we know exactly what we created because we
    created it ourselves. Later, pattern_first.py can compare what it
    FOUND against this file to see if it detected everything correctly.

    The JSON contains:
      - meta: how many cases, when generated, scenario distribution
      - cases: per-case detail (scenario type, blocking state, exception events)
      - summary: counts grouped by scenario type and exception type

    Returns the file path where the JSON was saved.
    """
    truth_cases = []
    scenario_counts: Dict[str, int] = {}
    exception_counts: Dict[str, int] = {}
    total_exceptions = 0

    for idx in range(1, num_instances + 1):
        scenario_type = SCENARIOS[(idx - 1) % len(SCENARIOS)]
        p = build_scenario(idx, scenario_type)
        events = build_events(p)

        # Pull out only the EXCEPTION events from this case
        exception_events = [
            {
                "event_type": ev["event_type"],
                "event_category": ev["event_category"],
                "node_id": ev["node_id"],
                "event_time": ev["event_time"],
                "actor_id": ev["actor_id"],
                "to_state": ev["to_state"],
            }
            for ev in events
            if ev["event_category"] == "EXCEPTION"
        ]

        # Count scenarios
        scenario_counts[scenario_type] = scenario_counts.get(scenario_type, 0) + 1

        # Count exception types
        for exc in exception_events:
            etype = exc["event_type"]
            exception_counts[etype] = exception_counts.get(etype, 0) + 1
            total_exceptions += 1

        truth_cases.append({
            "case_id": p["case_id"],
            "scenario_type": scenario_type,
            "current_blocking_node_id": p["current_blocking_node_id"],
            "current_blocking_state": p["current_blocking_state"],
            "has_exceptions": len(exception_events) > 0,
            "exception_events": exception_events,
            "opened_at": p["opened_at"],
            "last_event_time": p["last_event_time"],
        })

    truth = {
        "meta": {
            "generated_at": z(datetime.now(timezone.utc)),
            "generator": "scenario.py",
            "total_cases": num_instances,
            "total_exception_events": total_exceptions,
            "cases_with_exceptions": sum(
                1 for c in truth_cases if c["has_exceptions"]
            ),
        },
        "summary": {
            "by_scenario_type": scenario_counts,
            "by_exception_type": exception_counts,
        },
        "cases": truth_cases,
    }

    out_path = Path(__file__).parent / "scenario_truth.json"
    with open(out_path, "w") as f:
        json.dump(truth, f, indent=2)

    return str(out_path)


# ============================================================
# MAIN — The starting point of the whole program!
# ============================================================


def main() -> None:
    """
    The starting point of the whole program!

    1. Asks "How many STUCK shopping trip stories do you want?" (default: 30)
    2. Asks "How many should I save at a time?" (default: 100)
    3. Connects to the Neo4j database
    4. Sets up uniqueness rules (constraints) so no duplicates sneak in
    5. Builds each stuck case and saves them in batches
    6. Prints a success message when done!

    All cases created here are OPEN — they are stuck somewhere in the
    P2P process and haven't been completed yet. This is the key difference
    from raw.py, which creates fully COMPLETED cases.
    """
    raw = input("Enter number of open instances to create [default 30]: ").strip()
    num_instances = int(raw) if (raw.isdigit() and int(raw) > 0) else 30

    raw_batch = input("Enter batch size [default 100]: ").strip()
    batch_size = int(raw_batch) if (raw_batch.isdigit() and int(raw_batch) > 0) else 100

    with neo4j_driver() as driver:
        with driver.session() as session:
            # Step 1: Set up uniqueness rules (no duplicate IDs allowed!)
            session.execute_write(create_constraints)

            # Step 2: Build and save cases in batches for speed
            all_ids = list(range(1, num_instances + 1))
            for batch_ids in chunked(all_ids, batch_size):
                payloads = [build_case_payload(idx) for idx in batch_ids]
                session.execute_write(upsert_open_case_batch, payloads)

    print(f"Successfully upserted {num_instances} open P2P scenario cases.")
    print("Created labels: ProcessCase, ProcessNode, ProcessEdge, ProcessEvent")

    # Generate the truth file so pattern_first.py can check its work
    truth_path = generate_truth_file(num_instances)
    print(f"Truth file written to: {truth_path}")


if __name__ == "__main__":
    main()
