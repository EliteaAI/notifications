import json
from typing import Any, Optional, Sequence, TypedDict

from flask import request
from sqlalchemy import and_, func, or_

from ..models.all import Notification


class ParsedSearchToken(TypedDict):
    token: str
    event_types: list[str]
    status_clauses: list[Any]


# ---------------------------------------------------------------------------
# Bounds / whitelists
# ---------------------------------------------------------------------------

# Whitelist bound: even if the FE sends a huge list, never scan more than N
# JSONB keys per query. The FE-derived ALL_VARIABLE_KEYS list is small (<20)
# so this is a safety cap, not a functional limit.
_MAX_META_SEARCH_KEYS = 32

# Default set of JSONB meta keys scanned by free-text search. This mirrors
# the FE's `ALL_VARIABLE_KEYS` (union of all meta keys referenced by any
# notification template in src/components/notificationTemplates.js).
#
# MAINTENANCE: keep this list in sync with that file. When a new
# notification template introduces a new meta key that should be
# searchable, add it here. Changes are rare (only when templates change),
# so a hardcoded list is acceptable.
#
# The FE may still override the list by sending `meta_search_keys`,
# which lets a FE-only deploy (e.g. a new template) stay searchable
# without an immediate BE release. Unknown keys are dropped by the
# sanitizer, so stale overrides are harmless.
DEFAULT_META_SEARCH_KEYS = (
    "bucket_name",
    "index_name",
    "users",
    "project_name",
    "new_level",
    "token_name",
    "approver",
    "rejecter",
)

# Upper bound on per-type status-case filters to keep query complexity
# bounded even under a pathological client.
_MAX_STATUS_FILTERS = 32
_MAX_STATUS_VALUES_PER_FILTER = 16

# Any key a client may supply must match this — JSONB paths with quotes,
# braces, or whitespace are rejected outright to avoid surprising behavior.
_META_KEY_ALLOWED_CHARS = set(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
)


# ---------------------------------------------------------------------------
# Parameter parsing helpers
# ---------------------------------------------------------------------------

def parse_csv_param(name: str) -> list[str]:
    """
    Read a possibly repeated, possibly comma-joined query param into a
    deduplicated, order-preserving list. RTK Query serializes arrays as
    comma-joined strings, but we also accept the repeat-key form to stay
    tolerant of other clients.
    """
    raw_values = request.args.getlist(name)
    if not raw_values:
        return []
    collected: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        for piece in raw.split(","):
            piece = piece.strip()
            if piece and piece not in seen:
                seen.add(piece)
                collected.append(piece)
    return collected


def parse_search_tokens() -> list[ParsedSearchToken]:
    """
    Parse the `search_tokens` query param (URL-encoded JSON array). Each
    entry has shape:
        { "token": str, "eventTypes": [str], "eventTypeStatuses": [...] }

    Returns a list of dicts:
        { "token": str, "event_types": [str], "status_clauses": [SQL] }

    Malformed entries are silently dropped — endpoint is tolerant-by-design.
    """
    raw = request.args.get("search_tokens")
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except (ValueError, TypeError):
        return []
    if not isinstance(parsed, list):
        return []
    result: list[ParsedSearchToken] = []
    # Cap token count: a pathological client could otherwise force
    # O(tokens * meta_keys) predicate growth.
    for entry in parsed[:16]:
        if not isinstance(entry, dict):
            continue
        token = entry.get("token")
        if not isinstance(token, str) or not token or len(token) > 128:
            continue
        raw_types = entry.get("eventTypes") or []
        event_types = [t for t in raw_types if isinstance(t, str)] if isinstance(raw_types, list) else []
        raw_statuses = entry.get("eventTypeStatuses") or []
        status_clauses = []
        if isinstance(raw_statuses, list):
            for s in raw_statuses[:_MAX_STATUS_FILTERS]:
                clause = _build_status_filter_clause(s)
                if clause is not None:
                    status_clauses.append(clause)
        result.append({
            "token": token,
            "event_types": event_types,
            "status_clauses": status_clauses,
        })
    return result


# ---------------------------------------------------------------------------
# Meta-key sanitization
# ---------------------------------------------------------------------------

def _sanitize_meta_key(meta_key: Any) -> Optional[str]:
    """Return the key if it's a plain identifier of acceptable length, else None."""
    if not isinstance(meta_key, str) or not meta_key or len(meta_key) > 64:
        return None
    if any(ch not in _META_KEY_ALLOWED_CHARS for ch in meta_key):
        return None
    return meta_key


def sanitize_meta_keys(keys: list[str]) -> list[str]:
    """Drop anything that isn't a plain identifier; cap the total count."""
    safe: list[str] = []
    for key in keys:
        clean = _sanitize_meta_key(key)
        if clean is None:
            continue
        safe.append(clean)
        if len(safe) >= _MAX_META_SEARCH_KEYS:
            break
    return safe


# ---------------------------------------------------------------------------
# Synthetic status predicates
# ---------------------------------------------------------------------------

# Some notification types expose a synthetic "render state" that the FE
# computes from several real meta fields. The FE uses a `_`-prefixed meta
# key (e.g. `_renderState`) in its template; here we translate those
# synthetic keys into concrete SQL predicates over real meta fields, so a
# narrowed search from the FE ("is successfully ..." → only non-failed
# rows) resolves correctly in the database.
#
# Mapping shape:
#   (event_type, synthetic_meta_key) -> { case_key: SQLAlchemy predicate }
def _index_data_changed_predicates() -> dict[str, Any]:
    err = Notification.meta["error"].astext
    reindex = Notification.meta["reindex"].astext
    initiator = Notification.meta["initiator"].astext
    has_error = and_(err.isnot(None), func.trim(err) != "")
    no_error = or_(err.is_(None), func.trim(err) == "")
    is_reindex = reindex.isnot(None)
    return {
        "failed": has_error,
        "reindexed_scheduled": and_(no_error, is_reindex, initiator == "schedule"),
        "reindexed": and_(no_error, is_reindex, initiator != "schedule"),
        "created": and_(no_error, reindex.is_(None)),
    }


def _synthetic_status_predicates() -> dict[tuple[str, str], dict[str, Any]]:
    # Lazily built at first use so model imports stay cheap.
    return {
        ("index_data_changed", "_renderState"): _index_data_changed_predicates(),
    }


def build_search_filter(
    search_tokens: Sequence[ParsedSearchToken],
    meta_search_keys: Sequence[str],
) -> Optional[Any]:
    """
    Build a compound AND clause from a list of parsed search tokens.

    Each token contributes an OR clause across four axes:
      1. event_type ILIKE %token%
      2. event_type IN (FE-provided unconstrained types for this token)
      3. FE-provided status-case predicates (narrowed branches)
      4. meta[key].astext ILIKE %token% for each whitelisted key

    All per-token clauses are AND-ed together so every token must match.
    Returns None when the token list is empty.
    """
    if not search_tokens:
        return None
    per_token_clauses = []
    for entry in search_tokens:
        pattern = f"%{entry['token']}%"
        clauses = [Notification.event_type.ilike(pattern)]
        if entry['event_types']:
            clauses.append(Notification.event_type.in_(entry['event_types']))
        clauses.extend(entry['status_clauses'])
        for key in meta_search_keys:
            clauses.append(Notification.meta[key].astext.ilike(pattern))
        per_token_clauses.append(or_(*clauses))
    return and_(*per_token_clauses)


def _build_status_filter_clause(entry) -> Optional[Any]:
    """
    Translate one {type, metaKey, values} entry into a SQL predicate of the
    form `event_type = X AND (<per-case predicate> OR ...)`.

    Unknown types/keys/cases silently drop the entry — this endpoint is
    user-facing and tolerant-by-design; malformed input just yields fewer
    matches, never an error.
    """
    if not isinstance(entry, dict):
        return None
    event_type = entry.get("type")
    meta_key = entry.get("metaKey")
    values = entry.get("values")
    if not isinstance(event_type, str) or not isinstance(meta_key, str):
        return None
    if not isinstance(values, list) or not values:
        return None

    # Bound value-set size per entry.
    values = [v for v in values if isinstance(v, str)][:_MAX_STATUS_VALUES_PER_FILTER]
    if not values:
        return None

    if meta_key.startswith("_"):
        synth = _synthetic_status_predicates().get((event_type, meta_key))
        if not synth:
            return None
        case_clauses = [synth[v] for v in values if v in synth]
        if not case_clauses:
            return None
        return and_(Notification.event_type == event_type, or_(*case_clauses))

    clean_key = _sanitize_meta_key(meta_key)
    if clean_key is None:
        return None
    return and_(
        Notification.event_type == event_type,
        Notification.meta[clean_key].astext.in_(values),
    )
