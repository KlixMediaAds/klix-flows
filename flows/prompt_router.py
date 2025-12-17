import random
from typing import Sequence, Any, Optional


def _get(obj: Any, name: str, default: Any = None) -> Any:
    """
    Safely read attribute or dict key.
    Works with SQLAlchemy models, Pydantic, or plain dicts.
    """
    if obj is None:
        return default

    # Attribute first
    if hasattr(obj, name):
        value = getattr(obj, name)
        return default if value is None else value

    # Dict-style
    if isinstance(obj, dict):
        value = obj.get(name, default)
        return default if value is None else value

    return default


def weighted_random_choice(profiles: Sequence[Any]) -> Any:
    """
    Robust weighted random picker.
    Falls back to last profile if anything weird happens.
    """
    if not profiles:
        raise ValueError("weighted_random_choice() got empty profiles")

    weights = []
    for p in profiles:
        w = _get(p, "weight", 1)  # default weight = 1
        try:
            w = float(w)
        except (TypeError, ValueError):
            w = 1.0
        weights.append(max(w, 0.0))  # no negatives

    total = sum(weights) or len(profiles)
    r = random.uniform(0, total)
    acc = 0.0

    for p, w in zip(profiles, weights):
        acc += w
        if acc >= r:
            return p

    # Fallback
    return profiles[-1]


def pick_profile_for_lead(lead: Any, profiles: Sequence[Any]) -> Optional[Any]:
    """
    Segment-aware routing:
      - local_service → local + soft
      - ecom          → ecom + offer_explorer
      - otherwise     → all profiles

    We assume each profile has:
      - angle_id (str)
      - weight (optional, numeric-like)
    """
    if not profiles:
        return None

    business_type = _get(lead, "business_type", None) or _get(
        lead, "segment", None
    )

    # Helper to filter by angle_id
    def with_angles(*angles: str) -> list:
        targets = set(angles)
        return [
            p for p in profiles
            if _get(p, "angle_id") in targets
        ]

    local = with_angles("local_service_v1")
    soft = with_angles("soft_human_intro_v1", "soft_ugc")
    ecom = with_angles("smykm_v1", "short_visual", "pain_hunter_v1")
    offer_explorer = with_angles("offer_explorer_v1")
    generic = list(profiles)

    if business_type == "local_service" and local:
        pool = local + soft
    elif business_type == "ecom" and ecom:
        pool = ecom + offer_explorer
    else:
        pool = generic

    return weighted_random_choice(pool)
