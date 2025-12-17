#!/usr/bin/env python
import os
import textwrap

import psycopg2
from openai import OpenAI

DATABASE_URL = os.environ["DATABASE_URL"]
client = OpenAI()

USER_BRIEF = textwrap.dedent("""
You are writing the FIRST cold email in an outbound sequence.

Target:
- Brand: Glow Medical Aesthetics
- Niche: multi-location medspa
- Location: San Antonio, USA
- Contact: Founder

Goal:
Spark a short, low-friction reply about testing a small performance experiment.
Do NOT try to close a deal or list services.

Output format:
- First line: SUBJECT: <subject line>
- Blank line
- Then the email body only.
""").strip()


def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT name, angle_id, system_text, model_name
        FROM email_prompt_profiles
        WHERE is_active = TRUE
        ORDER BY name
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    print(f"Loaded {len(rows)} active prompt profiles.\n")

    for name, angle_id, system_text, model_name in rows:
        print("=" * 80)
        print(f"{name}  ({angle_id})")
        print("-" * 80)

        messages = [
            {"role": "system", "content": system_text},
            {"role": "user", "content": USER_BRIEF},
        ]

        resp = client.chat.completions.create(
            model=model_name,
            messages=messages,
            temperature=0.7,
        )

        content = resp.choices[0].message.content.strip()
        print(content)
        print()  # extra newline


if __name__ == "__main__":
    main()
