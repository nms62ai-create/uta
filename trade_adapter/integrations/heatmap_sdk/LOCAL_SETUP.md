# Local setup: heatmap-sdk + UTA on Binance UM Testnet

A concrete checklist to run heatmap-sdk's UI on your machine with UTA wired
in as the trade-execution backend. Defaults are picked so a fresh clone
ends up on Binance USD-M **testnet** — no real money at risk.

If you are reading this on the UTA repo, the patch referenced below
(`heatmap_sdk.patch`) lives next to this file:
`trade_adapter/integrations/heatmap_sdk/heatmap_sdk.patch`.

---

## 0. Prerequisites

- Python 3.11 (heatmap-sdk currently pins this; UTA accepts 3.11+).
- Either `uv` (recommended; UTA ships a `uv.lock`) or `pip` + `venv`.
- Binance Futures **Testnet** API key + secret. Get one at
  <https://testnet.binancefuture.com/> → API Management. The testnet
  account starts with fake USDT — you cannot lose real money.
- Account in **one-way (Net)** position mode, not Hedge. UTA's
  reconciliation logic assumes one-way.

---

## 1. Clone both repos side by side

```bash
mkdir -p ~/code && cd ~/code
git clone https://github.com/nms62ai-create/uta.git
# Replace the heatmap-sdk URL with wherever you keep it locally. The
# extracted source we developed against lives at
# /home/ubuntu/repos/heatmap-sdk/extracted/ in the dev VM.
git clone <heatmap-sdk-url> heatmap-sdk
```

Expected layout after this step:

```
~/code/
  uta/
  heatmap-sdk/
```

---

## 2. Apply the heatmap-sdk patch

The patch teaches heatmap-sdk's `LiveHeatmapService` to construct a UTA
stack on connect, pump prices and adaptive signals into it, replace
the inline `OrderExecutor` with UTA's `TradeAdapter`, and tear it down
on disconnect. It is gated behind `UTA_ENABLED=1` so heatmap-sdk runs
unchanged with the flag off.

```bash
cd ~/code/heatmap-sdk
git apply --check ~/code/uta/trade_adapter/integrations/heatmap_sdk/heatmap_sdk.patch
git apply         ~/code/uta/trade_adapter/integrations/heatmap_sdk/heatmap_sdk.patch
```

The patch is a unified diff. If `git apply` complains about whitespace
or context drift, try:

```bash
patch -p1 < ~/code/uta/trade_adapter/integrations/heatmap_sdk/heatmap_sdk.patch
```

The patch adds one new file (`app/uta_bridge.py`) and edits one
existing file (`app/ws_session.py`).

---

## 3. Install both packages in one venv

UTA is installed in editable mode so any edit in `~/code/uta/` is
picked up immediately — no rebuild required.

### Option A: `uv` (recommended)

```bash
cd ~/code/heatmap-sdk
uv venv --python 3.11
source .venv/bin/activate
uv pip install -r requirements.txt
uv pip install -e ~/code/uta            # editable install of UTA
```

### Option B: stdlib `venv` + `pip`

```bash
cd ~/code/heatmap-sdk
python3.11 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
pip install -e ~/code/uta               # editable install of UTA
```

Verify UTA is importable:

```bash
python -c "from trade_adapter.integrations.heatmap_sdk import bootstrap; print('UTA ok', bootstrap.build_stack.__name__)"
```

---

## 4. Configure environment variables

UTA reads no env vars on its own — they're all consumed by the
heatmap-sdk bridge added in step 2. Drop them into a `.env` (or export
them from your shell) before starting heatmap-sdk:

```bash
# REQUIRED — Binance Futures testnet credentials
export BINANCE_API_KEY="paste your testnet API key"
export BINANCE_API_SECRET="paste your testnet API secret"

# REQUIRED — flip the UTA wiring on. Defaults to off so the patch is a no-op.
export UTA_ENABLED=1

# Optional — testnet on by default. Set to 0 to go to mainnet (USE REAL MONEY).
export UTA_TESTNET=1

# Optional — where UTA persists its SQLite state (audit log, idempotency
# cache, schema_meta). Pick a writable path; ":memory:" works for a
# single-run smoke test but loses idempotency replay protection across restarts.
export UTA_DB_PATH=./uta_state.db

# Optional — default sizing for autotrade. The user can still override
# these from the heatmap-sdk UI's risk panel; these are the bootstrap
# defaults applied to every connect.
export UTA_DEFAULT_NOTIONAL_USD=50
export UTA_DEFAULT_SL_PCT=0.02     # 2.0% stop loss
export UTA_DEFAULT_TP_PCT=0.015    # 1.5% take profit
export UTA_AUTOTRADE_ENABLED=0     # 0 = manual confirm, 1 = auto-execute

# Optional — which exchange's adaptive signal drives autotrade?
# "binance" (default) or "bybit". Heatmap-sdk listens to both feeds;
# this picks which one is treated as the entry signal for UTA.
export UTA_SIGNAL_SOURCE=binance
```

If `UTA_ENABLED` is unset or `0`, the patched heatmap-sdk behaves
exactly like the unpatched version — UTA is bypassed entirely.

---

## 5. Run heatmap-sdk

```bash
cd ~/code/heatmap-sdk
source .venv/bin/activate
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Then open <http://localhost:8000> in a browser.

You should see log lines like:

```
INFO     heatmap-uta stack started (testnet=True, db=./uta_state.db)
INFO     uta_bridge: autotrade defaults notional=$50 SL=2.0% TP=1.5% enabled=False
```

If you don't see these, `UTA_ENABLED` is unset or the patch wasn't
applied — UTA is not wired in.

---

## 6. Smoke test on testnet

1. Pick a liquid symbol (e.g. `BTCUSDT`), set compression `1`, click **Connect**.
2. Wait for the order-book / heatmap to populate (a few seconds).
3. Click **Start heatmap**. You should see the live feed.
4. In the UI's risk panel, set notional / SL / TP if you want to
   override the env defaults, then flip **Autotrade** on.
5. Wait for an adaptive signal (`SELL_EXHAUSTION` or `BUY_EXHAUSTION`)
   with confidence above the entry filter threshold. The log should
   show one line per signal:

   ```
   INFO    HeatmapAutoTrader.on_adaptive_signal: forwarded LONG  symbol=BTCUSDT notional=50
   INFO    TradeAdapter.submit_signal accepted signal_id=… venue=binance_um symbol=BTCUSDT
   INFO    binance_um.ws_trade.order.place client_order_id=… → exchange_order_id=…
   ```

6. Check <https://testnet.binancefuture.com/en/futures/BTCUSDT> →
   **Open Orders** / **Positions**: the order placed by UTA should appear there.

If nothing happens after a few minutes, check:

- Did you flip autotrade on in the UI? (Default is off.)
- Are signals firing at all? The UI shows the latest signal in the
  assistant snapshot.
- Are your API key + secret correct? UTA logs the raw Binance error
  envelope on submit failure.

---

## 7. Tear down

- **Ctrl-C** in the uvicorn terminal: stops the server cleanly. The
  patch wires `stack.stop()` into FastAPI's shutdown handler so UTA
  flushes the audit log, closes the WS-trade connection, and deletes
  the listenKey before exiting.
- **Disconnect** in the UI: tears down the current connect's UTA
  stack but keeps the server running. Reconnecting builds a fresh one.

The SQLite file at `UTA_DB_PATH` survives across restarts. Delete it
manually if you want a clean slate (idempotency cache included).

---

## 8. Going to mainnet

Once testnet works and you have read [`INTEGRATION.md`](INTEGRATION.md)
end-to-end:

```bash
export UTA_TESTNET=0
export BINANCE_API_KEY="…mainnet…"
export BINANCE_API_SECRET="…mainnet…"
# Optional but recommended for first mainnet run:
export UTA_DEFAULT_NOTIONAL_USD=20   # smaller test trade
export UTA_AUTOTRADE_ENABLED=0       # require manual confirm
```

UTA's risk gate (`RiskConfig` → daily-loss cap + per-symbol qty cap +
kill switch) is on by default. To override the defaults, pass a
custom `risk_config` to `bootstrap.build_stack` — see
`trade_adapter/core/risk.py` for the available knobs.

---

## 9. Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `ImportError: trade_adapter` | UTA not installed in heatmap-sdk's venv | `pip install -e ~/code/uta` while heatmap-sdk's `.venv` is active |
| `ModuleNotFoundError: aiosqlite` | UTA's optional deps not installed | `pip install aiosqlite` or use the `uv pip install -e ~/code/uta[dev]` extra |
| `BinanceAuthError: -2014 API-key format invalid` | Wrong env vars or copied a spot-API key | Re-paste from <https://testnet.binancefuture.com/> → API Management |
| Stack starts but no orders fire | Autotrade off, or signal confidence below threshold | Check UI risk panel; check `entry_filter` log lines |
| `RiskKillSwitchEngaged` | The kill switch tripped (e.g. daily-loss cap hit) | Call `stack.reset_kill_switch()` from a debug shell, or restart |
| Submit fails with `-2019 Margin is insufficient` | Testnet wallet too small, or notional too large | Reduce `UTA_DEFAULT_NOTIONAL_USD`, or top up the testnet wallet from the faucet |

If anything else surfaces, grep the UTA logs for the `correlation_id`
that heatmap-sdk's UI shows in the order-status row — every UTA log
line tagged with that id traces the full signal→order→ack path.
