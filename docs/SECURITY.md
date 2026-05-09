# Security Model

The adapter handles real money. This document describes:

1. The threat model we're defending against (and explicitly NOT
   defending against).
2. How exchange API keys are stored, used, and protected.
3. How consumer authentication works.
4. The audit log.
5. Operational practices.

---

## Threat model

### In scope

- **Disk theft** (laptop loss, host compromise where the attacker has
  filesystem access but not memory). Encrypted-at-rest mitigates.
- **Unauthorized API consumers** (a service on the same host trying to
  POST signals). Bearer token + loopback-by-default mitigates.
- **Producer-side bugs causing absurd orders** (e.g. order with size
  100x equity due to a misplaced decimal). Emergency kill switch
  mitigates.
- **Replay attacks on signals** (network attacker re-sending a signal).
  Idempotency (cached `signal_id`) + TTL mitigates.
- **Log leakage** (logs accidentally exposed). Mandatory scrubbing
  before emission mitigates.

### Out of scope (v1.0)

- **Live memory dump while adapter runs.** If the attacker has root on
  the host while the process is running, they can read decrypted keys
  from memory. Defending against this requires secure enclaves
  (SGX/TPM/etc.) or separate key-signing daemon — both are v2.0+.
- **Compromised dependencies** (supply chain). We pin versions and
  audit `pip-audit`, but no SBOM signing in v1.0.
- **Side-channel attacks** (timing attacks on secret comparison, etc.).
  Out of scope; if your adversary is a nation-state with cache-timing
  research, hire them to advise instead.
- **Exchange compromise** (Binance/Bybit themselves get hacked). Not
  defendable.

---

## Exchange API key storage

### Where they live

- File path: `~/.universal-trade-adapter/secrets.enc` (configurable via
  `UTA_SECRETS_PATH` env).
- Format: encrypted JSON blob. Plaintext schema:

```json
{
  "version": 1,
  "exchanges": {
    "binance_um": { "api_key": "...", "api_secret": "...", "label": "main" },
    "bybit_linear": { "api_key": "...", "api_secret": "...", "label": "main" }
  },
  "consumer_tokens": {
    "<consumer_name>": "<bearer_token>"
  }
}
```

### How they're encrypted

- Symmetric encryption: `cryptography.fernet` (AES-128-CBC + HMAC-SHA256
  authenticated encryption).
- Key derivation: `Argon2id` with parameters
  `time_cost=3, memory_cost=64MB, parallelism=4` and a per-file random
  salt stored alongside the ciphertext.
- The fernet key is derived from the master password + salt at every
  decrypt operation. Not cached.

### Master password sourcing

In priority order:

1. `UTA_MASTER_PASSWORD` environment variable (suitable for systemd
   `EnvironmentFile=` with appropriate file permissions).
2. Interactive `getpass` prompt (works only on TTY; useful for manual
   ops and dev).

If neither is available, the adapter fails to start. There is no fallback
to "operate without keys".

### Lifecycle in process memory

- Plaintext key material is decrypted on-demand at the moment a REST
  request needs to be signed.
- Decrypted keys are passed as a `bytes` object to the request signer
  and discarded immediately after.
- No long-lived `ApiKeyHolder` object retains plaintext keys.
- No plaintext keys appear in `repr()`, `__dict__`, or any logged
  exception trace.

This is not bulletproof — Python's GC may keep `bytes` objects around
briefly — but it eliminates the easy attack surface of "process dump
includes a clearly-labeled long-lived secrets object".

### Logging discipline

A `structlog` processor scrubs known sensitive fields globally before
any log line is emitted:

```python
SENSITIVE_KEYS = {
    "api_key", "api_secret", "master_password",
    "password", "secret", "consumer_token", "bearer", "authorization",
}
```

Field values matching these keys are replaced with `"***SCRUBBED***"`.
This is enforced at the logger, not at the call site. Even if a developer
naively logs an exception that includes secrets, the scrubber catches it.

### Key rotation

- Adapter config supports `rotate-keys` CLI command:
  ```
  uta-cli rotate-keys --venue binance_um
  ```
- Procedure:
  1. Add new keys to the encrypted store under `binance_um.label="new"`.
  2. Adapter accepts both old and new keys via the same `binance_um`
     entry; specify `active_label` in config.
  3. Once new keys are confirmed working, delete the old entry.

Master password rotation:
- `uta-cli change-master-password` re-encrypts the secret store with a
  new master password. The old password is required.

---

## Consumer authentication

### Token issuance

- Each consumer (UI, bot, SDK) gets a per-consumer Bearer token at setup.
- Tokens are 256-bit random URL-safe strings.
- Tokens are stored alongside exchange keys in the encrypted file.
- Operator generates tokens via:
  ```
  uta-cli add-consumer --name "manual_ui"
  ```
  which prints the token once; if lost, must be regenerated.

### Token verification

Every REST request and WS upgrade requires:
```
Authorization: Bearer <token>
```

Token comparison uses `hmac.compare_digest` (constant-time).

### Roles (out of v1.0)

In v1.0, all tokens have full privileges. Roles (read-only / trade /
admin) are scoped for v1.1 if multi-consumer scenarios warrant them.

### Token revocation

`uta-cli revoke-consumer --name "manual_ui"` removes the token entry.
Future requests with that token return 401.

---

## Network exposure

### Default bind

`127.0.0.1:8080`. Loopback only by default. To expose externally:
1. Configure `bind = "0.0.0.0:8080"` in YAML config (explicit
   acknowledgement).
2. Place TLS-terminating reverse proxy (nginx, Caddy) in front.
3. Adapter does NOT do TLS termination itself in v1.0.

### Prometheus endpoint

`GET /metrics` is **not authenticated** by default and is exposed only
on loopback. Setting it on a non-loopback bind without a reverse-proxy
firewall is a misconfiguration. The adapter logs a startup warning if
`bind != 127.0.0.1` and metrics are enabled.

---

## Audit log

Every signal received is logged to SQLite `signals` table BEFORE any
processing. This is append-only — there is no DELETE in any query path.

The audit log records:
- `signal_id`, `received_at`, `source`
- Full signal payload (JSON, with metadata preserved)
- `accepted`, `rejection_reason`
- `resolved_intent` (if accepted)

Plus, `reconcile_events` table records every diff observed during
reconciliation. Both tables are intended for forensic review after any
incident.

For production deployments, consider periodically backing up the SQLite
file to an external location (S3, encrypted offsite). The adapter does
not do this automatically.

---

## Operational practices

### Recommended deployment

- Dedicated user account on the host (no shared use).
- Systemd unit with `EnvironmentFile=` for `UTA_MASTER_PASSWORD`,
  permissions 0600 owned by the adapter user.
- Filesystem permissions on `secrets.enc`: 0600 owned by adapter user.
- Log rotation: redirect stdout to a file with `logrotate`, keep 30
  days, encrypted at rest if disk encryption is in use.
- Process supervision: systemd `Restart=on-failure`, `RestartSec=10s`.
- Resource limits in systemd unit: `LimitNOFILE=65536`,
  `MemoryMax=2G`.

### What to do if keys are compromised

1. Disable keys on the exchange immediately (Binance/Bybit dashboards).
2. Generate fresh keys.
3. Run `uta-cli rotate-keys`.
4. Audit `signals` table for any unexpected entries since suspected
   compromise.
5. Audit exchange order history.
6. Change master password (`uta-cli change-master-password`).
7. Review all logs for the period of suspected compromise.

### What to do if the adapter crashes mid-trade

1. Adapter restarts (systemd).
2. On startup, reconciliation runs — pulls all open positions and
   orders from the exchanges.
3. Any orders the exchange has but local doesn't are logged as
   `external_order` and surfaced via `alert` events.
4. Any positions whose state diverges from local SQLite are normalized
   to exchange truth.
5. Native SL/TP orders survive the crash (they live on the exchange);
   no auto-flatten is attempted.
6. Once reconciliation completes, normal operation resumes.

### Backup strategy

- Encrypted `secrets.enc`: backed up to a separate secure location
  (encrypted USB, KMS-backed S3, etc.). Without the master password,
  the backup is unreadable.
- `state.db` (SQLite): backed up periodically. Used for forensics, not
  resumption — the source of truth on restart is always the exchange.
- Log files: shipped to an external log aggregator (Loki, ELK, etc.)
  for long-term retention and search.

---

## Reporting security issues

If you find a vulnerability, do NOT open a public issue. Email
the maintainer directly. Coordinated disclosure preferred.
