# app/sender_engine/mix.py
import random
from typing import List, Optional, Dict, Any

def build_balanced_labels(total: int, cold_slots: int, friendly_slots: int) -> List[str]:
    if cold_slots + friendly_slots != total:
        friendly_slots = max(0, total - cold_slots)
    labels: List[str] = []
    if friendly_slots == 0:
        labels = ['C'] * total
    elif cold_slots == 0:
        labels = ['F'] * total
    else:
        block = max(1, round(total / max(1, friendly_slots)))
        c_left, f_left = cold_slots, friendly_slots
        i = 0
        while c_left + f_left > 0:
            if (i % block == 0) and f_left > 0:
                labels.append('F'); f_left -= 1
            elif c_left > 0:
                labels.append('C'); c_left -= 1
            elif f_left > 0:
                labels.append('F'); f_left -= 1
            i += 1
    for j in range(1, len(labels)-1, 5):
        if random.random() < 0.25:
            labels[j], labels[j+1] = labels[j+1], labels[j]
    return labels

def pick_next_sender_for(kind: str, active_senders: list, last_sender_id: Optional[str]):
    if not active_senders:
        return None
    import random as _r
    start = _r.randrange(len(active_senders))
    best = None
    for k in range(len(active_senders)):
        s = active_senders[(start + k) % len(active_senders)]
        if s.remaining <= 0:
            continue
        if kind == 'C' and s._used_cold >= s._cap_cold:
            continue
        if kind == 'F' and s._used_friendly >= s._cap_friendly:
            continue
        if s.cfg["id"] == last_sender_id:
            best = best or s
            continue
        return s
    return best
