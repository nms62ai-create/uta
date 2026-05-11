"""Encrypted keystore for exchange API keys and consumer Bearer tokens.

Storage:
    Encrypted file at ``$HOME/.universal-trade-adapter/secrets.enc``
    (path overridable via ``UTA_SECRETS_PATH``). Plaintext schema:

        {
          "version": 1,
          "exchanges": {
            "binance_um":   {"api_key": "...", "api_secret": "...", "label": "main"},
            "bybit_linear": {"api_key": "...", "api_secret": "...", "label": "main"}
          },
          "consumer_tokens": {
            "<consumer_name>": "<bearer_token>"
          }
        }

Encryption:
    cryptography.fernet (AES-128-CBC + HMAC-SHA256). Key derived from
    master password + per-file random salt via Argon2id
    (time_cost=3, memory_cost=64MB, parallelism=4).

Master password sourcing (priority order):
    1. ``UTA_MASTER_PASSWORD`` environment variable
    2. Interactive ``getpass`` prompt at startup (TTY only)

Memory hygiene:
    - Plaintext keys are decrypted on-demand at REST sign time.
    - No long-lived plaintext attribute holds keys.
    - A structlog processor scrubs known sensitive field names before
      any log emission.

Modules:
    keystore.py     - SecretStore class: load, save, get_exchange_keys,
                      get_consumer_tokens, add_consumer_token,
                      rotate_keys, change_master_password.
    scrubber.py     - structlog processor enforcing log-line scrubbing
                      of sensitive fields globally.
"""
