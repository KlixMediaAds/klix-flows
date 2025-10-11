def build_subject(name:str|None, company:str|None) -> str:
    if name and company: return f"{name}, quick idea for {company}"
    if company: return f"Idea for {company}"
    return "Quick idea for you"
