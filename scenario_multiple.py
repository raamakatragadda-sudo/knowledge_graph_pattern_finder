from __future__ import annotations

import json
import os
import random
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

from dotenv import load_dotenv
from neo4j import GraphDatabase

sys.path.insert(0, str(Path(__file__).resolve().parent))


# ============================================================
# ENV
# ============================================================


def load_env_auto() -> None:
    p = Path(__file__).resolve()
    for parent in [p.parent] + list(p.parents):
        env_path = parent / ".env"
        if env_path.exists():
            load_dotenv(dotenv_path=env_path)
            return
    load_dotenv()


load_env_auto()


def req_env(name: str) -> str:
    v = (os.getenv(name) or "").strip()
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def neo4j_driver():
    return GraphDatabase.driver(
        req_env("NEO4J_URI"),
        auth=(req_env("NEO4J_USERNAME"), req_env("NEO4J_PASSWORD")),
    )


# ============================================================
# CONSTRAINTS
# ============================================================


def create_constraints(tx) -> None:
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
# HELPERS
# ============================================================


def z(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return (
        dt.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _offset_time(ts: Optional[str], **kwargs) -> Optional[str]:
    if not ts:
        return None
    s = ts.rstrip("Z") + "+00:00"
    try:
        dt = datetime.fromisoformat(s) + timedelta(**kwargs)
        return z(dt)
    except Exception:
        return None


def make_node_id(case_id: str, *parts: str) -> str:
    return "::".join([case_id, *parts])


def make_edge_id(
    case_id: str, rel_type: str, from_node_id: str, to_node_id: str
) -> str:
    return f"{case_id}::{rel_type}::{from_node_id}::{to_node_id}"


def make_event_id(case_id: str, node_id: str, event_type: str, event_time: str) -> str:
    return f"{case_id}::{node_id}::{event_type}::{event_time}"


def chunked(seq: List[int], size: int):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def build_common(idx: int) -> Dict[str, Any]:
    amount = float(1000 * idx)
    quantity = 10 * idx
    case_id = f"CASE_{idx}"

    return {
        "case_id": case_id,
        "pr_id": f"PR_{idx}",
        "po_id": f"PO_{idx}",
        "gr_id": f"GR_{idx}",
        "inv_id": f"INV_{idx}",
        "pay_id": f"PAY_{idx}",
        "req_actor": f"REQ_USER_{idx}",
        "buyer_actor": f"BUYER_{idx}",
        "receiver_actor": f"RECEIVER_{idx}",
        "ap_actor": f"AP_USER_{idx}",
        "pay_actor": f"PAY_USER_{idx}",
        "company_code": f"COMP_{((idx - 1) % 2) + 1}",
        "plant_id": f"PLANT_{((idx - 1) % 5) + 1}",
        "supplier_id": f"SUP_{((idx - 1) % 7) + 1}",
        "material_id": f"MAT_{((idx - 1) % 10) + 1}",
        "quantity": quantity,
        "amount": amount,
        "currency": "USD",
        "source_record_id": f"SRC_PR_{idx}",
        "root_node_id": make_node_id(case_id, "P2P"),
        "pr_node_id": make_node_id(case_id, "PR"),
        "po_node_id": make_node_id(case_id, "PO"),
        "gr_node_id": make_node_id(case_id, "GR"),
        "inv_node_id": make_node_id(case_id, "INVOICE"),
        "pay_node_id": make_node_id(case_id, "PAYMENT"),
    }


# ============================================================
# CLEANUP EXISTING GENERIC CASE GRAPH
# ============================================================


def reset_case_graph_batch(tx, case_ids: List[str]) -> None:
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
# OPEN SCENARIOS
# ============================================================

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

# ============================================================
# MULTI-STAGE RANDOM SCENARIO
# Each case can accumulate delays at several stages before
# getting stuck at one final blocking point.
# ============================================================

# Ordered checkpoints in the P2P flow
_STAGE_SEQ = [
    "PR_SUBMITTED",        # waiting for PR approval
    "PO_APPROVED",         # waiting for PO issuance
    "PO_ISSUED",           # waiting for goods receipt
    "GOODS_RECEIVED",      # waiting for invoice
    "INVOICE_RECEIVED",    # waiting for invoice validation
    "INVOICE_VALIDATED",   # waiting for payment
]

# Normal dwell range (hours) for each transition
_NORMAL_H = {
    "PR_SUBMITTED":      (1,  3),
    "PO_APPROVED":       (3,  6),
    "PO_ISSUED":         (48, 96),   # 2-4 days for goods delivery
    "GOODS_RECEIVED":    (12, 24),
    "INVOICE_RECEIVED":  (4,  8),
    "INVOICE_VALIDATED": (24 * 15, 24 * 30),  # NET30 payment
}

# Delay dwell range (hours) — statistically significant vs baseline
_DELAY_H = {
    "PR_SUBMITTED":      (24 * 5,  24 * 25),
    "PO_APPROVED":       (24 * 8,  24 * 35),
    "PO_ISSUED":         (24 * 14, 24 * 60),
    "GOODS_RECEIVED":    (24 * 5,  24 * 20),
    "INVOICE_RECEIVED":  (24 * 7,  24 * 30),
    "INVOICE_VALIDATED": (24 * 40, 24 * 90),
}

# Probability of delay at each stage when the case passes through it
_DELAY_PROB = 0.35

# Stuck-stage weights — controls how often cases get stuck at each point
_STUCK_WEIGHTS = [20, 20, 10, 15, 20, 15]  # mirrors real P2P patterns


def _rng(idx: int) -> random.Random:
    """Seeded RNG per case index — reproducible but varied."""
    return random.Random(idx * 31337)


def build_multistage_scenario(idx: int) -> Dict[str, Any]:
    """
    Builds a case with random delays at multiple stages plus one stuck point.

    For each stage before the stuck point:
      - with probability _DELAY_PROB the transition is delayed (slow dwell)
      - otherwise it runs at the normal pace

    At the stuck point the case stops — no completion event is written,
    so anomaly.py detects it via now() - last_event_time.

    Earlier slow dwells are captured in StepDwell / StageDwell nodes by
    operationald.py once the events are ingested.
    """
    rng = _rng(idx)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    base = build_common(idx)

    # Pick which stages get a delay and where the case gets stuck
    stuck_stage = rng.choices(_STAGE_SEQ, weights=_STUCK_WEIGHTS, k=1)[0]
    stuck_idx = _STAGE_SEQ.index(stuck_stage)

    delayed_stages: set = set()
    for stage in _STAGE_SEQ[:stuck_idx]:
        if rng.random() < _DELAY_PROB:
            delayed_stages.add(stage)

    # Build the timeline forward from opened_at
    opened_at = now - timedelta(days=2 + (idx * 13) % 87, hours=(idx * 5) % 12)

    def _hours(stage: str, delayed: bool) -> float:
        lo, hi = (_DELAY_H if delayed else _NORMAL_H)[stage]
        return rng.uniform(lo, hi)

    t = opened_at
    pr_created   = t
    t += timedelta(hours=rng.uniform(1, 2))
    pr_submitted = t

    # Which exception events to add
    add_match_fail     = False
    add_price_variance = False

    pr_approved = po_created = po_approved = po_issued = None
    gr_received = inv_received = inv_validated = pay_paid = None

    pr_status = "SUBMITTED"
    po_status = gr_status = inv_status = pay_status = None

    def _advance(stage: str) -> datetime:
        nonlocal t
        delayed = stage in delayed_stages
        t = t + timedelta(hours=_hours(stage, delayed))
        return t

    # PR approval
    if stuck_stage == "PR_SUBMITTED":
        pass  # stuck here, no approval
    else:
        pr_approved = _advance("PR_SUBMITTED")
        pr_status = "APPROVED"
        t += timedelta(hours=rng.uniform(3, 6))   # PR→PO handoff (always fast)
        po_created = t

    # PO approval (always fast once PO exists)
    if po_created and stuck_stage not in ("PR_SUBMITTED",):
        t += timedelta(hours=rng.uniform(4, 8))
        po_approved = t
        po_status = "APPROVED"

    # PO issuance
    if po_approved:
        if stuck_stage == "PO_APPROVED":
            pass  # stuck here
        else:
            po_issued = _advance("PO_APPROVED")
            po_status = "ISSUED"

    # Goods receipt
    if po_issued:
        if stuck_stage == "PO_ISSUED":
            pass
        else:
            gr_received = _advance("PO_ISSUED")
            gr_status = "RECEIVED"

    # Invoice receipt
    if gr_received:
        if stuck_stage == "GOODS_RECEIVED":
            pass
        else:
            inv_received = _advance("GOODS_RECEIVED")
            inv_status = "RECEIVED"
            if rng.random() < 0.25:   # 25% chance of match failure exception
                add_match_fail = True

    # Invoice validation
    if inv_received:
        if stuck_stage == "INVOICE_RECEIVED":
            pass
        else:
            inv_validated = _advance("INVOICE_RECEIVED")
            inv_status = "VALIDATED"

    # Payment
    if inv_validated:
        if stuck_stage == "INVOICE_VALIDATED":
            pass
        else:
            pay_paid = _advance("INVOICE_VALIDATED")
            pay_status = "PAID"

    # Price variance: attach to PO stage randomly
    if po_created and rng.random() < 0.15:
        add_price_variance = True

    # Determine actual last event time and blocking point
    last_event_time = (
        pay_paid or inv_validated or inv_received or
        gr_received or po_issued or po_approved or
        po_created or pr_approved or pr_submitted
    )

    blocking_map = {
        "PR_SUBMITTED":      (base["pr_node_id"],  "PR_SUBMITTED"),
        "PO_APPROVED":       (base["po_node_id"],  "PO_APPROVED"),
        "PO_ISSUED":         (base["po_node_id"],  "PO_ISSUED"),
        "GOODS_RECEIVED":    (base["gr_node_id"],  "GOODS_RECEIVED"),
        "INVOICE_RECEIVED":  (base["inv_node_id"], "INVOICE_RECEIVED"),
        "INVOICE_VALIDATED": (base["inv_node_id"], "INVOICE_VALIDATED"),
    }
    current_blocking_node_id, current_blocking_state = blocking_map[stuck_stage]

    _pt_cycle = ["NET30", "NET45", "ZB30", "2-10NET30"]
    payment_terms = _pt_cycle[idx % 4]
    net_days = {"NET30": 30, "NET45": 45, "ZB30": 30, "2-10NET30": 30}[payment_terms]
    discount_pct = 2.0 if payment_terms == "2-10NET30" else 0.0
    ordered_qty = base["quantity"]
    received_qty = ordered_qty if idx % 5 != 0 else int(ordered_qty * 0.8)

    # Derive a human-readable scenario label
    delay_labels = sorted(delayed_stages) or ["none"]
    scenario_label = f"MULTISTAGE|stuck={stuck_stage}|delays={'_'.join(delay_labels)}"

    p = dict(base)
    p.update({
        "scenario_type":            scenario_label,
        "payment_terms":            payment_terms,
        "net_days":                 net_days,
        "discount_pct":             discount_pct,
        "ordered_qty":              ordered_qty,
        "received_qty":             received_qty,
        "invoice_qty":              received_qty,
        "opened_at":                z(opened_at),
        "last_event_time":          z(last_event_time),
        "closed_at":                None,
        "pr_created":               z(pr_created),
        "pr_submitted":             z(pr_submitted),
        "pr_approved":              z(pr_approved),
        "pr_status":                pr_status,
        "po_created":               z(po_created),
        "po_approved":              z(po_approved),
        "po_issued":                z(po_issued),
        "po_status":                po_status,
        "gr_received":              z(gr_received),
        "gr_status":                gr_status,
        "inv_received":             z(inv_received),
        "inv_validated":            z(inv_validated),
        "inv_status":               inv_status,
        "pay_paid":                 z(pay_paid),
        "pay_status":               pay_status,
        "current_blocking_node_id": current_blocking_node_id,
        "current_blocking_state":   current_blocking_state,
        # carry exception flags for build_events
        "_add_match_fail":          add_match_fail,
        "_add_price_variance":      add_price_variance,
    })
    return p


def build_scenario(idx: int, scenario_type: str) -> Dict[str, Any]:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    base = build_common(idx)

    opened_at = now - timedelta(days=1 + (idx * 13) % 89, hours=(idx * 5) % 12)

    pr_created = opened_at
    pr_submitted = pr_created + timedelta(hours=1 + (idx % 3))

    pr_approved = None
    po_created = None
    po_approved = None
    po_issued = None
    gr_received = None
    inv_received = None
    inv_validated = None
    pay_paid = None

    pr_status = "SUBMITTED"
    po_status = None
    gr_status = None
    inv_status = None
    pay_status = None

    if scenario_type == "PR_APPROVAL_DELAY":
        last_event_time = pr_submitted
        current_blocking_node_id = base["pr_node_id"]
        current_blocking_state = "PR_SUBMITTED"

    elif scenario_type == "PO_ISSUE_DELAY":
        pr_approved = pr_submitted + timedelta(hours=3 + (idx % 5))
        po_created = pr_approved + timedelta(hours=4)
        po_approved = po_created + timedelta(hours=5 + (idx % 4))
        pr_status = "APPROVED"
        po_status = "APPROVED"
        last_event_time = po_approved
        current_blocking_node_id = base["po_node_id"]
        current_blocking_state = "PO_APPROVED"

    elif scenario_type == "GR_DELAY":
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
        pr_approved = pr_submitted + timedelta(hours=3 + (idx % 4))
        po_created = pr_approved + timedelta(hours=3)
        pr_status = "APPROVED"
        po_status = "CREATED"
        last_event_time = po_created
        current_blocking_node_id = base["po_node_id"]
        current_blocking_state = "PO_CREATED"

    else:
        raise ValueError(f"Unknown scenario_type: {scenario_type}")

    _pt_cycle = ["NET30", "NET45", "ZB30", "2-10NET30"]
    payment_terms = _pt_cycle[idx % 4]
    net_days = {"NET30": 30, "NET45": 45, "ZB30": 30, "2-10NET30": 30}[payment_terms]
    discount_pct = 2.0 if payment_terms == "2-10NET30" else 0.0
    ordered_qty = base["quantity"]
    received_qty = ordered_qty if idx % 5 != 0 else int(ordered_qty * 0.8)
    invoice_qty = received_qty

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
            "closed_at": None,
            "pr_created": z(pr_created),
            "pr_submitted": z(pr_submitted),
            "pr_approved": z(pr_approved),
            "pr_status": pr_status,
            "po_created": z(po_created),
            "po_approved": z(po_approved),
            "po_issued": z(po_issued),
            "po_status": po_status,
            "gr_received": z(gr_received),
            "gr_status": gr_status,
            "inv_received": z(inv_received),
            "inv_validated": z(inv_validated),
            "inv_status": inv_status,
            "pay_paid": z(pay_paid),
            "pay_status": pay_status,
            "current_blocking_node_id": current_blocking_node_id,
            "current_blocking_state": current_blocking_state,
        }
    )
    return p


# ============================================================
# BATCH UPSERTS
# ============================================================


def upsert_process_cases_batch(tx, rows: List[Dict[str, Any]]) -> None:
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


def build_nodes(p: Dict[str, Any]) -> List[Dict[str, Any]]:
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
                "source_system": "synthetic_open_loader",
                "owner_actor": owner_actor,
                "attributes": json.dumps(attrs),
            }
        )

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
    edges: List[Dict[str, Any]] = []

    def add_edge(
        rel_type: str,
        from_node_id: str,
        to_node_id: str,
        effective_from: Optional[str],
        attrs: Dict[str, Any],
    ) -> None:
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

    add_edge("CONTAINS", p["root_node_id"], p["pr_node_id"], p["pr_created"], {})

    if p["po_created"]:
        add_edge("CONTAINS", p["root_node_id"], p["po_node_id"], p["po_created"], {})
        add_edge(
            "GENERATED_FROM", p["po_node_id"], p["pr_node_id"], p["po_created"], {}
        )

    if p["gr_received"]:
        add_edge("CONTAINS", p["root_node_id"], p["gr_node_id"], p["gr_received"], {})
        add_edge("FOLLOWS", p["gr_node_id"], p["po_node_id"], p["gr_received"], {})

    if p["inv_received"]:
        add_edge("CONTAINS", p["root_node_id"], p["inv_node_id"], p["inv_received"], {})
        add_edge(
            "MATCHED_TO",
            p["inv_node_id"],
            p["po_node_id"],
            p["inv_received"],
            {"match_basis": "PO"},
        )
        if p["gr_received"]:
            add_edge(
                "MATCHED_TO",
                p["inv_node_id"],
                p["gr_node_id"],
                p["inv_received"],
                {"match_basis": "GR"},
            )

    if p["pay_paid"]:
        add_edge("CONTAINS", p["root_node_id"], p["pay_node_id"], p["pay_paid"], {})
        add_edge("SETTLES", p["pay_node_id"], p["inv_node_id"], p["pay_paid"], {})

    return edges


def build_events(p: Dict[str, Any]) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []

    def add_event(
        node_id: str,
        event_type: str,
        event_time: Optional[str],
        actor_id: Optional[str],
        to_state: Optional[str],
        event_category: str = "BUSINESS",
    ) -> None:
        if not event_time:
            return
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

    add_event(p["pr_node_id"], "PR_CREATED", p["pr_created"], p["req_actor"], "DRAFT")
    add_event(
        p["pr_node_id"], "PR_SUBMITTED", p["pr_submitted"], p["req_actor"], "SUBMITTED"
    )
    add_event(
        p["pr_node_id"], "PR_APPROVED", p["pr_approved"], p["req_actor"], "APPROVED"
    )

    add_event(
        p["po_node_id"], "PO_CREATED", p["po_created"], p["buyer_actor"], "CREATED"
    )
    add_event(
        p["po_node_id"], "PO_APPROVED", p["po_approved"], p["buyer_actor"], "APPROVED"
    )
    add_event(p["po_node_id"], "PO_ISSUED", p["po_issued"], p["buyer_actor"], "ISSUED")

    add_event(
        p["gr_node_id"],
        "GOODS_RECEIVED",
        p["gr_received"],
        p["receiver_actor"],
        "RECEIVED",
    )

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

    add_event(
        p["pay_node_id"], "PAYMENT_COMPLETED", p["pay_paid"], p["pay_actor"], "PAID"
    )

    # Exception events — triggered by scenario type OR multistage flags
    match_fail = (
        p["scenario_type"] == "MATCH_FAILURE_BLOCK"
        or p.get("_add_match_fail", False)
    )
    price_var = (
        p["scenario_type"] == "PRICE_VARIANCE_BLOCK"
        or p.get("_add_price_variance", False)
    )

    if match_fail and p.get("inv_received"):
        add_event(
            p["inv_node_id"],
            "MATCH_FAILED",
            _offset_time(p["inv_received"], minutes=45),
            p["ap_actor"],
            "MATCH_FAILED",
            "EXCEPTION",
        )

    if price_var and p.get("po_created"):
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
# BATCH BUILD
# ============================================================


def build_case_payload(idx: int) -> Dict[str, Any]:
    p = build_multistage_scenario(idx)

    process_case = {
        "case_id": p["case_id"],
        "root_node_id": p["root_node_id"],
        "opened_at": p["opened_at"],
        "closed_at": p["closed_at"],
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

    nodes = build_nodes(p)
    edges = build_edges(p)
    events = build_events(p)

    return {
        "case_id": p["case_id"],
        "process_case": process_case,
        "nodes": nodes,
        "edges": edges,
        "events": events,
    }


def upsert_open_case_batch(tx, payloads: List[Dict[str, Any]]) -> None:
    case_ids = [p["case_id"] for p in payloads]
    reset_case_graph_batch(tx, case_ids)

    all_cases: List[Dict[str, Any]] = []
    all_nodes: List[Dict[str, Any]] = []
    all_edges: List[Dict[str, Any]] = []
    all_events: List[Dict[str, Any]] = []

    for payload in payloads:
        all_cases.append(payload["process_case"])
        all_nodes.extend(payload["nodes"])
        all_edges.extend(payload["edges"])
        all_events.extend(payload["events"])

    upsert_process_cases_batch(tx, all_cases)
    upsert_process_nodes_batch(tx, all_nodes)
    upsert_process_edges_batch(tx, all_edges)
    upsert_process_events_batch(tx, all_events)


# ============================================================
# MAIN
# ============================================================


def main() -> None:
    raw = input("Enter number of open instances to create [default 30]: ").strip()
    num_instances = int(raw) if (raw.isdigit() and int(raw) > 0) else 30

    raw_batch = input("Enter batch size [default 100]: ").strip()
    batch_size = int(raw_batch) if (raw_batch.isdigit() and int(raw_batch) > 0) else 100

    with neo4j_driver() as driver:
        with driver.session() as session:
            session.execute_write(create_constraints)

            all_ids = list(range(1, num_instances + 1))
            for batch_ids in chunked(all_ids, batch_size):
                payloads = [build_case_payload(idx) for idx in batch_ids]
                session.execute_write(upsert_open_case_batch, payloads)

    print(f"Successfully upserted {num_instances} open P2P scenario cases.")
    print("Created labels: ProcessCase, ProcessNode, ProcessEdge, ProcessEvent")


if __name__ == "__main__":
    main()
