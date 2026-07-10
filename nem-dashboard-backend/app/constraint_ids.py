"""Parse AEMO NEM constraint IDs into category/region/kind metadata.

Grammar (see tests/unit/test_constraint_ids.py for the real-fixture derivation):
  - F_... -> FCAS (island token after F_ maps to region(s)).
  - Leading region letters (N/V/Q/S/T, singly or compound e.g. QN, SVML, TVBL)
    followed by a limit-type separator (>, >>, ^^, ^, ::) -> network.
  - Leading region letters followed by a plain "_" (no limit-type separator)
    -> network, kind derived from ROCOF/STRGTH keywords or "local".
  - Anything else -> "other".
"""
import re
from typing import Dict, List, Optional

REGION_LETTERS = {"N": "NSW1", "V": "VIC1", "Q": "QLD1", "S": "SA1", "T": "TAS1"}
MAINLAND_REGIONS = ["NSW1", "QLD1", "VIC1", "SA1"]
SHORT_NAMES = {"NSW1": "NSW", "VIC1": "VIC", "QLD1": "QLD", "SA1": "SA", "TAS1": "TAS"}

# Ordered longest-match-first so ">>" / "^^" aren't swallowed by ">" / "^".
_OPERATORS = [(">>", "post-contingent"), (">", "thermal"), ("^^", "voltage"), ("^", "voltage"), ("::", "transient")]

_FCAS_TOKEN_RE = re.compile(r"F_([A-Za-z]+)")


def _fcas_regions(token: str) -> List[str]:
    """Map the island token after F_ to its region list (MAIN/I are special-cased)."""
    if token == "MAIN":
        return list(MAINLAND_REGIONS)
    if token == "I":
        return []
    if token.startswith("T"):
        return ["TAS1"]
    return []


def _fcas_label(regions: List[str], token: str) -> str:
    if token == "MAIN":
        return "FCAS · mainland"
    if not regions:
        return "FCAS · system"
    return f"FCAS · {'/'.join(SHORT_NAMES[r] for r in regions)}"


def _leading_region_run(cid: str):
    """Consume leading single-letter region codes (N/V/Q/S/T); returns (regions, index)."""
    regions = []
    i = 0
    while i < len(cid) and cid[i] in REGION_LETTERS:
        regions.append(REGION_LETTERS[cid[i]])
        i += 1
    return regions, i


def _skip_compound_suffix_letters(cid: str, i: int) -> int:
    """Skip extra name letters (e.g. the "ML"/"BL" in SVML/TVBL) before the separator."""
    while i < len(cid) and cid[i].isalpha():
        i += 1
    return i


def _match_operator(cid: str, i: int):
    for op, kind in _OPERATORS:
        if cid.startswith(op, i):
            return op, kind
    return None, None


def _right_side_region(rest: str):
    """A single region-letter token immediately before the next "_" adds a region (e.g. V^^N_...)."""
    underscore_idx = rest.find("_")
    token = rest[:underscore_idx] if underscore_idx != -1 else rest
    if token and all(c in REGION_LETTERS for c in token):
        return [REGION_LETTERS[c] for c in token]
    return []


def _underscore_kind(cid: str) -> str:
    """Kind for the no-real-separator network variant, from keyword scan of the raw id."""
    if "ROCOF" in cid:
        return "rocof"
    if "STRGTH" in cid:
        return "system-strength"
    return "local"


def _network_label(regions: List[str], kind: str) -> str:
    region_part = "↔".join(SHORT_NAMES[r] for r in regions) if regions else "?"
    return f"{region_part} · {kind}"


def _parse_network(cid: str) -> Optional[Dict]:
    regions, i = _leading_region_run(cid)
    if not regions:
        return None

    i = _skip_compound_suffix_letters(cid, i)
    op, opkind = _match_operator(cid, i)
    if op is not None:
        i += len(op)
        kind = opkind
    elif i < len(cid) and cid[i] == "_":
        i += 1
        kind = None  # resolved below via keyword scan, once the full id is known
    else:
        return None

    regions = regions + _right_side_region(cid[i:])
    regions = list(dict.fromkeys(regions))  # dedupe, preserve encounter order
    if kind is None:
        kind = _underscore_kind(cid)

    return {"category": "network", "regions": regions, "kind": kind, "label": _network_label(regions, kind)}


def parse_constraint_id(cid: str) -> Dict:
    """Classify an AEMO constraint ID into category/regions/kind/label."""
    if cid.startswith("F_"):
        match = _FCAS_TOKEN_RE.match(cid)
        token = match.group(1) if match else ""
        regions = _fcas_regions(token)
        return {"category": "fcas", "regions": regions, "kind": "fcas", "label": _fcas_label(regions, token)}

    network = _parse_network(cid)
    if network is not None:
        return network

    return {"category": "other", "regions": [], "kind": None, "label": cid}
