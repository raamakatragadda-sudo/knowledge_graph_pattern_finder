from __future__ import annotations

import json
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from neo4j import GraphDatabase


# =====================================
# ENV
# =====================================


def load_env_auto():
    """
    Finds and loads our secret settings file (.env).
    Think of .env like a locked diary that has passwords and addresses in it.
    This function looks in the current folder, then the parent folder, then the
    grandparent folder, etc., until it finds the diary. Once found, it reads it
    so the rest of the program can use those secrets.
    """
    p = Path(__file__).resolve()
    for parent in [p.parent] + list(p.parents):
        env = parent / ".env"
        if env.exists():
            load_dotenv(env)
            return
    load_dotenv()


# Actually run the function above as soon as this file is loaded,
# so the secrets are available right away.
load_env_auto()


def req_env(name: str) -> str:
    """
    Grabs a specific secret from the diary (.env file).
    If the secret isn't there, it throws a tantrum (raises an error)
    because the program can't work without it.
    """
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var {name}")
    return v


def neo4j_driver():
    """
    Creates a "phone line" (connection) to our Neo4j database.
    Neo4j is like a big whiteboard where we draw circles (nodes)
    and arrows (edges) to show how things are connected.
    We need the address (URI), username, and password to connect.
    """
    return GraphDatabase.driver(
        req_env("NEO4J_URI"),
        auth=(req_env("NEO4J_USERNAME"), req_env("NEO4J_PASSWORD")),
    )


# =====================================
# HELPERS
# =====================================


def to_iso_z(dt: datetime) -> str:
    """
    Turns a date+time into a neat text string like "2025-01-15T08:00:00Z".
    The "Z" at the end means "this time is in UTC" (the world's shared clock).
    It also chops off the tiny microseconds because we don't need that much detail.
    """
    return dt.replace(microsecond=0).isoformat() + "Z"


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
    It combines the case, what kind of arrow it is, and what two circles it connects.
    Example: "CASE_1::CONTAINS::CASE_1::P2P::CASE_1::PR"
    """
    return f"{case_id}::{rel_type}::{from_node_id}::{to_node_id}"


def make_event_id(case_id: str, node_id: str, event_type: str, event_time: str) -> str:
    """
    Makes a unique name tag for an event (something that happened at a specific time).
    Example: "CASE_1::CASE_1::PR::PR_CREATED::2025-01-15T08:00:00Z"
    """
    return f"{case_id}::{node_id}::{event_type}::{event_time}"


# =====================================
# CONSTRAINTS
# =====================================


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


# =====================================
# BATCH UPSERT HELPERS
# =====================================
# "Upsert" means: "If this thing already exists, update it. If not, create it."
# Think of it like a sticker book — if you already have the sticker, you replace
# it with a newer version. If you don't have it, you stick a new one in.
# "Batch" means we do many at once instead of one at a time (much faster!).


def upsert_process_cases_batch(tx, rows: List[Dict]) -> None:
    """
    Saves a bunch of "cases" to the database at once.
    A "case" is like one complete shopping trip from start to finish —
    from asking "can I buy this?" all the way to paying for it.
    Each case has an ID, a type (P2P = "Procure to Pay"), a status, and timestamps.
    """
    if not rows:
        return
    tx.run(
        """
        UNWIND $rows AS row
        MERGE (c:ProcessCase {case_id: row.case_id})
        SET
          c.process_type = row.process_type,
          c.root_node_id = row.root_node_id,
          c.status = row.status,
          c.opened_at = row.opened_at,
          c.closed_at = row.closed_at,
          c.last_event_time = row.last_event_time,
          c.attributes = row.attributes
        """,
        rows=rows,
    )


def upsert_process_nodes_batch(tx, rows: List[Dict]) -> None:
    """
    Saves a bunch of "nodes" to the database at once.
    Nodes are the circles on our whiteboard. Each one represents a step
    in the shopping trip — like the purchase request, the purchase order,
    the goods receipt, the invoice, or the payment.
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


def upsert_process_edges_batch(tx, rows: List[Dict]) -> None:
    """
    Saves a bunch of "edges" to the database at once.
    Edges are the arrows on our whiteboard that connect the circles.
    They show relationships like "this Purchase Order was GENERATED FROM this
    Purchase Request" or "this Invoice is MATCHED TO this Purchase Order."
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


def upsert_process_events_batch(tx, rows: List[Dict]) -> None:
    """
    Saves a bunch of "events" to the database at once.
    Events are like diary entries — "PR was created at 8am", "PO was approved at 2pm".
    Each event records WHAT happened, WHEN it happened, and WHO did it.
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


# =====================================
# CASE BUILD
# =====================================


def build_case_payload(idx: int) -> Dict:
    """
    Builds ALL the data for one complete "shopping trip" (case).
    Think of it like writing a full story about buying something:

    The story has 5 chapters:
      1. Purchase Request (PR) — "Hey boss, can I buy this?"
      2. Purchase Order (PO)  — "OK, let's officially order it from the supplier."
      3. Goods Receipt (GR)   — "The stuff arrived at our warehouse!"
      4. Invoice (INV)        — "The supplier sent us a bill."
      5. Payment (PAY)        — "We paid the bill."

    Each chapter has timestamps so we know when things happened.
    The 'idx' number makes each case slightly different (different times,
    amounts, suppliers, etc.) so they're not all identical copies.
    """

    # --- TIMELINE: Figure out when each step happens ---
    # We spread cases across ~1.5 years starting Jan 1, 2025.
    # The math with % (modulo) makes each case land on a different date.
    _window_start = datetime(2025, 1, 1, 8, 0, 0)
    _window_days = 547
    base_time = _window_start + timedelta(
        days=(idx * 37) % _window_days,
        hours=(idx * 7) % 9,
    )

    # Chapter 1 timestamps: Purchase Request
    pr_created = base_time
    pr_submitted = pr_created + timedelta(hours=1 + (idx % 3))      # submitted 1-3 hours later
    pr_approved = pr_submitted + timedelta(hours=2 + (idx % 6))     # approved 2-7 hours after that

    # Chapter 2 timestamps: Purchase Order
    po_created = pr_approved + timedelta(hours=3 + (idx % 4))       # created 3-6 hours after PR approval
    po_approved = po_created + timedelta(hours=1 + (idx % 3))       # approved 1-3 hours later
    po_issued = po_approved + timedelta(hours=2 + (idx % 3))        # issued (sent to supplier) 2-4 hours later

    # Chapter 3-5 timestamps: Goods Receipt → Invoice → Payment
    gr_received = po_issued + timedelta(days=3 + (idx % 4))         # goods arrive 3-6 days later
    inv_received = gr_received + timedelta(days=1, hours=(idx % 5)) # invoice comes 1+ days after goods
    inv_validated = inv_received + timedelta(hours=4 + (idx % 4))   # invoice checked 4-7 hours later
    pay_paid = inv_validated + timedelta(days=1 + (idx % 3), hours=(idx % 4))  # paid 1-3 days later

    case_id = f"CASE_{idx}"

    # --- BUSINESS DETAILS: amounts, quantities, payment terms ---
    amount = float(1000 * idx)         # each case has a different dollar amount
    quantity = 10 * idx                # and a different quantity of items

    # Payment terms cycle through 4 options (like rotating through 4 flavors of ice cream)
    _pt_cycle = ["NET30", "NET45", "ZB30", "2-10NET30"]
    payment_terms = _pt_cycle[idx % 4]
    net_days = {"NET30": 30, "NET45": 45, "ZB30": 30, "2-10NET30": 30}[payment_terms]
    discount_pct = 2.0 if payment_terms == "2-10NET30" else 0.0  # 2% discount if you pay early

    ordered_qty = quantity
    # Every 5th case: we only received 80% of what we ordered (a short delivery!)
    received_qty = quantity if idx % 5 != 0 else int(quantity * 0.8)
    invoice_qty = received_qty
    # Invoice amount is proportional to what we actually received
    invoice_amount = round(float(amount) * received_qty / ordered_qty, 2)
    payment_due_date = to_iso_z(inv_received + timedelta(days=net_days))
    # Only the "2-10NET30" terms have a discount deadline (pay within 10 days for 2% off)
    discount_date = (
        to_iso_z(inv_received + timedelta(days=10))
        if payment_terms == "2-10NET30"
        else None
    )

    # --- IDENTIFIERS: give everything a name ---
    company_code = f"COMP_{((idx - 1) % 2) + 1}"  # alternates between COMP_1 and COMP_2
    plant_id = f"PLANT_{idx}"
    supplier_id = f"SUP_{idx}"
    material_id = f"MAT_{idx}"

    # --- NODE IDS: unique name tags for each circle on the whiteboard ---
    root_node_id = make_node_id(case_id, "P2P")       # the "main" circle that holds everything
    pr_node_id = make_node_id(case_id, "PR")           # Purchase Request circle
    po_node_id = make_node_id(case_id, "PO")           # Purchase Order circle
    gr_node_id = make_node_id(case_id, "GR")           # Goods Receipt circle
    inv_node_id = make_node_id(case_id, "INVOICE")     # Invoice circle
    pay_node_id = make_node_id(case_id, "PAYMENT")     # Payment circle

    # Convert all timestamps to text strings
    times = {
        "pr_created": to_iso_z(pr_created),
        "pr_submitted": to_iso_z(pr_submitted),
        "pr_approved": to_iso_z(pr_approved),
        "po_created": to_iso_z(po_created),
        "po_approved": to_iso_z(po_approved),
        "po_issued": to_iso_z(po_issued),
        "gr_received": to_iso_z(gr_received),
        "inv_received": to_iso_z(inv_received),
        "inv_validated": to_iso_z(inv_validated),
        "pay_paid": to_iso_z(pay_paid),
    }

    # --- THE CASE ITSELF: the "book cover" for our shopping trip story ---
    process_case = {
        "case_id": case_id,
        "process_type": "P2P",                        # P2P = Procure-to-Pay
        "root_node_id": root_node_id,
        "status": "COMPLETED",                        # this story is finished
        "opened_at": times["pr_created"],             # story started when PR was created
        "closed_at": times["pay_paid"],               # story ended when payment was made
        "last_event_time": times["pay_paid"],
        "attributes": json.dumps(                     # extra info stored as a JSON string
            {
                "company_code": company_code,
                "supplier_id": supplier_id,
                "plant_id": plant_id,
                "material_id": material_id,
                "currency": "USD",
            }
        ),
    }

    # --- NODES: the circles on our whiteboard ---
    # Each node is one step in the process. They all belong to this case
    # and are children of the root node (like chapters in the book).
    nodes = [
        # The ROOT node — the "book" itself that contains all chapters
        {
            "node_id": root_node_id,
            "case_id": case_id,
            "parent_node_id": None,                   # no parent — this IS the top
            "node_type": "CASE_ROOT",
            "node_subtype": "P2P",
            "entity_id": None,
            "entity_type": None,
            "level_num": 0,                           # level 0 = the very top
            "status": "COMPLETED",
            "state_code": "PAYMENT_COMPLETED",
            "start_time": times["pr_created"],
            "end_time": times["pay_paid"],
            "source_system": "synthetic_loader",      # this data was made by our program
            "owner_actor": None,
            "attributes": json.dumps(
                {
                    "amount": amount,
                    "currency": "USD",
                    "company_code": company_code,
                    "supplier_id": supplier_id,
                    "plant_id": plant_id,
                    "material_id": material_id,
                    "quantity": quantity,
                }
            ),
        },
        # Chapter 1: Purchase Request — "Can I buy this?"
        {
            "node_id": pr_node_id,
            "case_id": case_id,
            "parent_node_id": root_node_id,
            "node_type": "BUSINESS_OBJECT",
            "node_subtype": "REQUISITION",
            "entity_id": f"PR_{idx}",
            "entity_type": "PR",
            "level_num": 1,                           # level 1 = chapter level
            "status": "APPROVED",
            "state_code": "PR_APPROVED",
            "start_time": times["pr_created"],
            "end_time": times["pr_approved"],
            "source_system": "synthetic_loader",
            "owner_actor": f"REQ_USER_{idx}",         # the person who asked to buy something
            "attributes": json.dumps(
                {
                    "entity_line_id": "1",
                    "company_code": company_code,
                    "plant_id": plant_id,
                    "supplier_id": supplier_id,
                    "material_id": material_id,
                    "quantity": quantity,
                    "amount": amount,
                    "currency": "USD",
                    "source_record_id": f"SRC_PR_{idx}",
                }
            ),
        },
        # Chapter 2: Purchase Order — "Dear supplier, please send us this stuff"
        {
            "node_id": po_node_id,
            "case_id": case_id,
            "parent_node_id": root_node_id,
            "node_type": "BUSINESS_OBJECT",
            "node_subtype": "PURCHASE_ORDER",
            "entity_id": f"PO_{idx}",
            "entity_type": "PO",
            "level_num": 1,
            "status": "ISSUED",
            "state_code": "PO_ISSUED",
            "start_time": times["po_created"],
            "end_time": times["po_issued"],
            "source_system": "synthetic_loader",
            "owner_actor": f"BUYER_{idx}",            # the buyer who placed the order
            "attributes": json.dumps(
                {
                    "entity_line_id": "1",
                    "parent_entity_id": f"PR_{idx}",  # this PO came from this PR
                    "parent_entity_type": "PR",
                    "company_code": company_code,
                    "plant_id": plant_id,
                    "supplier_id": supplier_id,
                    "material_id": material_id,
                    "quantity": quantity,
                    "amount": amount,
                    "currency": "USD",
                    "payment_terms": payment_terms,
                    "net_days": net_days,
                    "discount_pct": discount_pct,
                }
            ),
        },
        # Chapter 3: Goods Receipt — "The package arrived!"
        {
            "node_id": gr_node_id,
            "case_id": case_id,
            "parent_node_id": root_node_id,
            "node_type": "BUSINESS_OBJECT",
            "node_subtype": "GOODS_RECEIPT",
            "entity_id": f"GR_{idx}",
            "entity_type": "GR",
            "level_num": 1,
            "status": "RECEIVED",
            "state_code": "GOODS_RECEIVED",
            "start_time": times["gr_received"],
            "end_time": times["gr_received"],         # goods receipt is instant
            "source_system": "synthetic_loader",
            "owner_actor": f"RECEIVER_{idx}",         # the warehouse person who checked the delivery
            "attributes": json.dumps(
                {
                    "entity_line_id": "1",
                    "parent_entity_id": f"PO_{idx}",
                    "parent_entity_type": "PO",
                    "plant_id": plant_id,
                    "supplier_id": supplier_id,
                    "material_id": material_id,
                    "ordered_qty": ordered_qty,
                    "received_qty": received_qty,     # might be less than ordered!
                }
            ),
        },
        # Chapter 4: Invoice — "Here's the bill from the supplier"
        {
            "node_id": inv_node_id,
            "case_id": case_id,
            "parent_node_id": root_node_id,
            "node_type": "BUSINESS_OBJECT",
            "node_subtype": "INVOICE",
            "entity_id": f"INV_{idx}",
            "entity_type": "Invoice",
            "level_num": 1,
            "status": "VALIDATED",
            "state_code": "INVOICE_VALIDATED",
            "start_time": times["inv_received"],
            "end_time": times["inv_validated"],
            "source_system": "synthetic_loader",
            "owner_actor": f"AP_USER_{idx}",          # Accounts Payable person who checks the bill
            "attributes": json.dumps(
                {
                    "entity_line_id": "1",
                    "parent_entity_id": f"PO_{idx}",
                    "secondary_parent_entity_id": f"GR_{idx}",
                    "supplier_id": supplier_id,
                    "invoice_qty": invoice_qty,
                    "invoice_amount": invoice_amount,
                    "amount": amount,
                    "currency": "USD",
                    "payment_due_date": payment_due_date,
                    "discount_date": discount_date,
                }
            ),
        },
        # Chapter 5: Payment — "We paid the supplier!"
        {
            "node_id": pay_node_id,
            "case_id": case_id,
            "parent_node_id": root_node_id,
            "node_type": "BUSINESS_OBJECT",
            "node_subtype": "PAYMENT",
            "entity_id": f"PAY_{idx}",
            "entity_type": "Payment",
            "level_num": 1,
            "status": "PAID",
            "state_code": "PAYMENT_COMPLETED",
            "start_time": times["pay_paid"],
            "end_time": times["pay_paid"],
            "source_system": "synthetic_loader",
            "owner_actor": f"PAY_USER_{idx}",         # the person who sent the money
            "attributes": json.dumps(
                {
                    "parent_entity_id": f"INV_{idx}",
                    "parent_entity_type": "Invoice",
                    "supplier_id": supplier_id,
                    "amount": amount,
                    "currency": "USD",
                }
            ),
        },
    ]

    # --- EDGES: the arrows connecting the circles ---
    # There are two kinds:
    #   1. "CONTAINS" arrows: Root → each chapter (like a table of contents)
    #   2. Relationship arrows: show the flow, like PO came from PR, GR follows PO, etc.
    edges = [
        # "The book CONTAINS the PR chapter"
        {
            "edge_id": make_edge_id(case_id, "CONTAINS", root_node_id, pr_node_id),
            "case_id": case_id,
            "from_node_id": root_node_id,
            "to_node_id": pr_node_id,
            "relationship_type": "CONTAINS",
            "effective_from": times["pr_created"],
            "effective_to": None,
            "attributes": json.dumps({}),
        },
        # "The book CONTAINS the PO chapter"
        {
            "edge_id": make_edge_id(case_id, "CONTAINS", root_node_id, po_node_id),
            "case_id": case_id,
            "from_node_id": root_node_id,
            "to_node_id": po_node_id,
            "relationship_type": "CONTAINS",
            "effective_from": times["po_created"],
            "effective_to": None,
            "attributes": json.dumps({}),
        },
        # "The book CONTAINS the GR chapter"
        {
            "edge_id": make_edge_id(case_id, "CONTAINS", root_node_id, gr_node_id),
            "case_id": case_id,
            "from_node_id": root_node_id,
            "to_node_id": gr_node_id,
            "relationship_type": "CONTAINS",
            "effective_from": times["gr_received"],
            "effective_to": None,
            "attributes": json.dumps({}),
        },
        # "The book CONTAINS the Invoice chapter"
        {
            "edge_id": make_edge_id(case_id, "CONTAINS", root_node_id, inv_node_id),
            "case_id": case_id,
            "from_node_id": root_node_id,
            "to_node_id": inv_node_id,
            "relationship_type": "CONTAINS",
            "effective_from": times["inv_received"],
            "effective_to": None,
            "attributes": json.dumps({}),
        },
        # "The book CONTAINS the Payment chapter"
        {
            "edge_id": make_edge_id(case_id, "CONTAINS", root_node_id, pay_node_id),
            "case_id": case_id,
            "from_node_id": root_node_id,
            "to_node_id": pay_node_id,
            "relationship_type": "CONTAINS",
            "effective_from": times["pay_paid"],
            "effective_to": None,
            "attributes": json.dumps({}),
        },
        # "The Purchase Order was GENERATED FROM the Purchase Request"
        # (You can't order without asking first!)
        {
            "edge_id": make_edge_id(case_id, "GENERATED_FROM", po_node_id, pr_node_id),
            "case_id": case_id,
            "from_node_id": po_node_id,
            "to_node_id": pr_node_id,
            "relationship_type": "GENERATED_FROM",
            "effective_from": times["po_created"],
            "effective_to": None,
            "attributes": json.dumps({}),
        },
        # "The Goods Receipt FOLLOWS the Purchase Order"
        # (Stuff arrives after you order it)
        {
            "edge_id": make_edge_id(case_id, "FOLLOWS", gr_node_id, po_node_id),
            "case_id": case_id,
            "from_node_id": gr_node_id,
            "to_node_id": po_node_id,
            "relationship_type": "FOLLOWS",
            "effective_from": times["gr_received"],
            "effective_to": None,
            "attributes": json.dumps({}),
        },
        # "The Invoice is MATCHED TO the Purchase Order"
        # (We check: does the bill match what we ordered?)
        {
            "edge_id": make_edge_id(case_id, "MATCHED_TO", inv_node_id, po_node_id),
            "case_id": case_id,
            "from_node_id": inv_node_id,
            "to_node_id": po_node_id,
            "relationship_type": "MATCHED_TO",
            "effective_from": times["inv_received"],
            "effective_to": None,
            "attributes": json.dumps({"match_basis": "PO"}),
        },
        # "The Invoice is MATCHED TO the Goods Receipt"
        # (We also check: does the bill match what we actually received?)
        {
            "edge_id": make_edge_id(case_id, "MATCHED_TO", inv_node_id, gr_node_id),
            "case_id": case_id,
            "from_node_id": inv_node_id,
            "to_node_id": gr_node_id,
            "relationship_type": "MATCHED_TO",
            "effective_from": times["inv_received"],
            "effective_to": None,
            "attributes": json.dumps({"match_basis": "GR"}),
        },
        # "The Payment SETTLES the Invoice"
        # (Paying the bill = done!)
        {
            "edge_id": make_edge_id(case_id, "SETTLES", pay_node_id, inv_node_id),
            "case_id": case_id,
            "from_node_id": pay_node_id,
            "to_node_id": inv_node_id,
            "relationship_type": "SETTLES",
            "effective_from": times["pay_paid"],
            "effective_to": None,
            "attributes": json.dumps({}),
        },
    ]

    # --- EVENTS: the diary entries of what happened and when ---
    # Each tuple is: (which node, what happened, when, category, who did it, new status)
    event_specs: List[Tuple[str, str, str, str, Optional[str], Optional[str]]] = [
        # PR events — the life of a Purchase Request
        (pr_node_id, "PR_CREATED", times["pr_created"], "BUSINESS", f"REQ_USER_{idx}", "DRAFT"),
        (pr_node_id, "PR_SUBMITTED", times["pr_submitted"], "BUSINESS", f"REQ_USER_{idx}", "SUBMITTED"),
        (pr_node_id, "PR_APPROVED", times["pr_approved"], "BUSINESS", f"REQ_USER_{idx}", "APPROVED"),

        # PO events — the life of a Purchase Order
        (po_node_id, "PO_CREATED", times["po_created"], "BUSINESS", f"BUYER_{idx}", "CREATED"),
        (po_node_id, "PO_APPROVED", times["po_approved"], "BUSINESS", f"BUYER_{idx}", "APPROVED"),
        (po_node_id, "PO_ISSUED", times["po_issued"], "BUSINESS", f"BUYER_{idx}", "ISSUED"),

        # GR event — goods arrived
        (gr_node_id, "GOODS_RECEIVED", times["gr_received"], "BUSINESS", f"RECEIVER_{idx}", "RECEIVED"),

        # Invoice events
        (inv_node_id, "INVOICE_RECEIVED", times["inv_received"], "BUSINESS", f"AP_USER_{idx}", "RECEIVED"),
        (inv_node_id, "INVOICE_VALIDATED", times["inv_validated"], "BUSINESS", f"AP_USER_{idx}", "VALIDATED"),

        # Payment event — money sent!
        (pay_node_id, "PAYMENT_COMPLETED", times["pay_paid"], "BUSINESS", f"PAY_USER_{idx}", "PAID"),
    ]

    # --- EXCEPTION EVENTS: sometimes things go wrong! ---
    # Every 5th case: the price didn't match, so it got flagged and then fixed
    if idx % 5 == 0:
        event_specs += [
            (
                po_node_id,
                "PRICE_VARIANCE_FLAGGED",
                to_iso_z(po_created + timedelta(minutes=20)),
                "EXCEPTION",
                f"BUYER_{idx}",
                "PRICE_VARIANCE",
            ),
            (
                po_node_id,
                "PRICE_VARIANCE_RESOLVED",
                to_iso_z(po_created + timedelta(minutes=50)),
                "EXCEPTION",
                f"BUYER_{idx}",
                "APPROVED",
            ),
        ]

    # Every 7th case: the invoice didn't match the order, had to be corrected
    if idx % 7 == 0:
        event_specs += [
            (
                inv_node_id,
                "MATCH_FAILED",
                to_iso_z(inv_received + timedelta(hours=1)),
                "EXCEPTION",
                f"AP_USER_{idx}",
                "MATCH_FAILED",
            ),
            (
                inv_node_id,
                "MATCH_CORRECTED",
                to_iso_z(inv_received + timedelta(hours=2)),
                "EXCEPTION",
                f"AP_USER_{idx}",
                "VALIDATED",
            ),
        ]

    # Every 4th case: the invoice got blocked (held up) and then unblocked
    if idx % 4 == 0:
        event_specs += [
            (
                inv_node_id,
                "INVOICE_BLOCKED",
                to_iso_z(inv_received + timedelta(hours=1, minutes=30)),
                "EXCEPTION",
                f"AP_USER_{idx}",
                "BLOCKED",
            ),
            (
                inv_node_id,
                "INVOICE_UNBLOCKED",
                to_iso_z(inv_received + timedelta(hours=3)),
                "EXCEPTION",
                f"AP_USER_{idx}",
                "VALIDATED",
            ),
        ]

    # Turn each event spec tuple into a proper dictionary
    events = []
    for (
        node_id,
        event_type,
        event_time,
        event_category,
        actor_id,
        to_state,
    ) in event_specs:
        events.append(
            {
                "event_id": make_event_id(case_id, node_id, event_type, event_time),
                "case_id": case_id,
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

    # Return the entire story: the case, its chapters (nodes), arrows (edges), and diary (events)
    return {
        "process_case": process_case,
        "nodes": nodes,
        "edges": edges,
        "events": events,
    }


# =====================================
# MAIN UPSERT
# =====================================


def upsert_case_batch(tx, payloads: List[Dict]) -> None:
    """
    Takes a bunch of complete case stories and saves them ALL to the database.
    It collects all the cases, nodes, edges, and events into big lists,
    then saves each list in one go (like mailing a big package instead of
    many small letters — it's faster!).
    """
    all_cases: List[Dict] = []
    all_nodes: List[Dict] = []
    all_edges: List[Dict] = []
    all_events: List[Dict] = []

    for payload in payloads:
        all_cases.append(payload["process_case"])
        all_nodes.extend(payload["nodes"])
        all_edges.extend(payload["edges"])
        all_events.extend(payload["events"])

    upsert_process_cases_batch(tx, all_cases)
    upsert_process_nodes_batch(tx, all_nodes)
    upsert_process_edges_batch(tx, all_edges)
    upsert_process_events_batch(tx, all_events)


def chunked(seq: List[int], size: int):
    """
    Splits a big list into smaller chunks.
    Like cutting a long sandwich into bite-sized pieces.
    Example: chunked([1,2,3,4,5], 2) → [1,2], [3,4], [5]
    """
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


# =====================================
# MAIN
# =====================================


def main() -> None:
    """
    The starting point of the whole program!

    1. Asks "How many shopping trip stories do you want me to create?" (default: 30)
    2. Asks "How many should I save at a time?" (default: 100)
    3. Connects to the Neo4j database
    4. Sets up the rules (constraints) so no duplicates sneak in
    5. Builds each story and saves them in batches
    6. Prints a success message when done!

    All cases created here are COMPLETED — the full journey from request to payment.
    """
    raw = input("Enter number of instances to create [default 30]: ").strip()
    num_instances = int(raw) if (raw.isdigit() and int(raw) > 0) else 30

    raw_batch = input("Enter batch size [default 100]: ").strip()
    batch_size = int(raw_batch) if (raw_batch.isdigit() and int(raw_batch) > 0) else 100

    with neo4j_driver() as driver:
        with driver.session() as session:
            # Step 1: Set up uniqueness rules
            session.execute_write(create_constraints)

            # Step 2: Build and save cases in batches
            all_ids = list(range(1, num_instances + 1))
            for batch_ids in chunked(all_ids, batch_size):
                payloads = [build_case_payload(idx) for idx in batch_ids]
                session.execute_write(upsert_case_batch, payloads)

    print(f"Successfully upserted {num_instances} generic P2P process cases.")
    print("Created labels: ProcessCase, ProcessNode, ProcessEdge, ProcessEvent")


if __name__ == "__main__":
    main()
