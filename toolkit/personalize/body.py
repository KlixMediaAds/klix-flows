from textwrap import dedent
def build_body_html(name:str|None, company:str|None, email:str|None) -> str:
    who = name or "there"; comp = company or "your brand"
    return dedent(f"""
    <div style="font-family:system-ui,Segoe UI,Arial">
      <p>Hey {who},</p>
      <p>We’ve been helping small brands like {comp} test short-form ad creatives fast.
         If you want, I can mock a <b>free 15-sec concept</b> using your product shots.</p>
      <p>Zero pressure—if it flops, you owe nothing. If it hits, we’ll scale.</p>
      <p>— Klix Media<br/><small>{email or ''}</small></p>
    </div>
    """).strip()
