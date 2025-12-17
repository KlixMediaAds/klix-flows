from __future__ import annotations

import os
import sys
from typing import List

from google_auth_oauthlib.flow import InstalledAppFlow

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

# Where we will store per-account token JSONs
GMAIL_TOKENS_DIR = os.getenv("GMAIL_TOKENS_DIR", "/etc/klix/gmail_tokens")


def _ensure_tokens_dir() -> None:
    os.makedirs(GMAIL_TOKENS_DIR, exist_ok=True)


def _get_client_secrets_path() -> str:
    """
    Return the path to the OAuth client secrets JSON.

    By default we look at /etc/klix/gmail_client_secret.json, but this can be
    overridden via GOOGLE_OAUTH_CLIENT_SECRETS_FILE if needed.
    """
    env_path = os.getenv("GOOGLE_OAUTH_CLIENT_SECRETS_FILE")
    if env_path:
        if not os.path.exists(env_path):
            raise FileNotFoundError(
                f"GOOGLE_OAUTH_CLIENT_SECRETS_FILE={env_path} does not exist."
            )
        return env_path

    default_path = "/etc/klix/gmail_client_secret.json"
    if not os.path.exists(default_path):
        raise FileNotFoundError(
            f"Client secrets file not found at '{default_path}'. "
            "Download it from Google Cloud Console (OAuth client ID of type "
            "'Desktop app') and place it there, or set "
            "GOOGLE_OAUTH_CLIENT_SECRETS_FILE."
        )
    return default_path


def generate_token_for_account(account: str) -> None:
    _ensure_tokens_dir()
    client_secrets_file = _get_client_secrets_path()

    print(f"\n=== Generating Gmail token for account '{account}' ===")
    print("A URL will be printed; open it in your browser, grant access,")
    print("then paste the authorization code back here.\n")

    flow = InstalledAppFlow.from_client_secrets_file(
        client_secrets_file,
        scopes=GMAIL_SCOPES,
    )

    # Manual console-style flow: show URL, user pastes code.
    auth_url, _ = flow.authorization_url(
        prompt="consent",
        access_type="offline",
        include_granted_scopes="true",
    )

    print("Visit this URL in your browser:")
    print(auth_url)
    print("\nAfter approving, you should see a code. Paste it below.")
    code = input("Authorization code: ").strip()

    flow.fetch_token(code=code)
    creds = flow.credentials

    token_path = os.path.join(GMAIL_TOKENS_DIR, f"{account}.json")
    with open(token_path, "w", encoding="utf-8") as f:
        f.write(creds.to_json())

    print(f"Saved token for '{account}' to {token_path}")


def main(argv: List[str]) -> None:
    if len(argv) < 2:
        print("Usage: python -m klix.google.gmail_token_bootstrap <account1> [account2 ...]")
        sys.exit(1)

    accounts = argv[1:]
    for account in accounts:
        generate_token_for_account(account)


if __name__ == "__main__":  # pragma: no cover
    main(sys.argv)
