import re
EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
def is_valid_format(email:str) -> bool: return bool(EMAIL_RE.match((email or "").strip()))
def validate(email:str):
    if not email: return False, "empty"
    if not is_valid_format(email): return False, "format"
    return True, "ok"
