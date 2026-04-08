"""
helpers.py -- Shared utility functions used across all stages.

Contains timestamp parsing, time-delta calculation, median, and
human-readable time formatting. These are pure functions with no
side effects and no dependency on Neo4j or PrefixSpan.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, List


def parse_iso(ts: Optional[str]) -> Optional[datetime]:
    """
    Parse an ISO-8601 timestamp string into a datetime object.
    Handles the trailing 'Z' (UTC) format. Returns None if the
    input is empty or unparseable.
    """
    if not ts:
        return None
    ts = ts.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(ts)
    except ValueError:
        return None


def minutes_between(a: Optional[str], b: Optional[str]) -> Optional[float]:
    """
    Compute the number of minutes between two ISO timestamp strings.
    Returns None if either timestamp is missing or unparseable.
    """
    dt_a = parse_iso(a)
    dt_b = parse_iso(b)
    if dt_a is None or dt_b is None:
        return None
    return (dt_b - dt_a).total_seconds() / 60.0


def median(values: List[float]) -> float:
    """
    Compute the median of a list of floats.
    Returns 0.0 for an empty list.
    """
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    if n % 2 == 1:
        return s[n // 2]
    return (s[n // 2 - 1] + s[n // 2]) / 2.0


def fmt_time(minutes: float) -> str:
    """
    Format a duration in minutes into a human-readable string.
    Examples: "45 min", "3.2 hrs", "5.1 days"
    """
    if abs(minutes) >= 1440:
        return f"{minutes / 1440:.1f} days"
    if abs(minutes) >= 60:
        return f"{minutes / 60:.1f} hrs"
    return f"{minutes:.0f} min"
