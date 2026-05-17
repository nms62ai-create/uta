# Local Setup — UTA + heatmap-sdk

Three steps. The launcher does everything: creates the virtualenv,
extracts heatmap-sdk, applies the integration patch, installs Python
dependencies, prompts for your Binance API keys, and starts the
server with your browser pointed at it.

## 1. Clone the repo

```bash
git clone https://github.com/nms62ai-create/uta.git
cd uta
```

Heatmap-sdk ships inside this repo as `heatmap-sdk.zip` at the root —
you do **not** need to clone it separately.

## 2. Run the launcher

### Windows

Double-click `launcher\start.bat`, or from `cmd` / PowerShell:

```bat
launcher\start.bat
```

### Linux / macOS

```bash
bash launcher/start.sh
```

On the **first run** the launcher will:

1. Verify Python 3.11+ is on PATH (`py -3` on Windows, `python3` on Unix).
2. Create `./.venv` and bootstrap pip inside it.
3. Extract `heatmap-sdk.zip` to `./heatmap-sdk/`.
4. Apply `trade_adapter/integrations/heatmap_sdk/heatmap_sdk.patch` to
   the extracted heatmap-sdk. Uses GNU `patch` if available, otherwise
   a bundled pure-Python applier so Windows users do not need extra
   tools.
5. `pip install -e .` (UTA, editable) and
   `pip install -r heatmap-sdk/requirements.txt`.
6. Prompt you for:
   - `BINANCE_API_KEY` (visible)
   - `BINANCE_API_SECRET` (hidden)
   - testnet Y/n (default Y — strongly recommended for the first run)
   - default trade size $ (default 50)
   - default Stop-Loss % (default 2)
   - default Take-Profit % (default 1.5)
   - autotrade enabled y/N (default N — manual confirm in UI)

   Answers are written to `.env` (mode 0600 on POSIX). On every
   subsequent run the launcher reads `.env` and skips the prompts.
7. Launch `uvicorn app.main:app` and open your default browser at
   `http://127.0.0.1:8000/`.

On **subsequent runs** every step except #7 is a no-op (idempotent),
so the launcher boots straight into the server.

> Need testnet keys? Sign up at
> <https://testnet.binancefuture.com/en/>, mint an API key, and set
> the account to **one-way** position mode (not Hedge).

## 3. Use the UI

1. Wait for the browser tab to load `http://127.0.0.1:8000/`.
2. Pick a symbol (e.g. `BTCUSDT`) in the controls panel.
3. Set the **$ notional**, **Stop-Loss %**, **Take-Profit %**,
   **min confidence** in the autotrade panel.
4. Click **Connect** → heatmap-sdk subscribes to Binance market
   streams and UTA wires up REST + WS-trade + USER_DATA_STREAM.
5. Toggle **Autotrade ON**. The adaptive SDK now feeds signals into
   UTA; UTA's autotrader gates them (confidence, dedupe, kill-switch,
   one-position-at-a-time) and forwards survivors as entry orders
   with parallel SL/TP children.
6. Toggle **Autotrade OFF** when you want to stop opening new
   positions. Existing positions remain open — close them by hand
   in the UI or in Binance, or by clicking **Disconnect** which
   tears the whole stack down.
7. Press **Ctrl-C** in the terminal to stop the server. The launcher
   forwards SIGINT to uvicorn which calls FastAPI shutdown hooks, so
   the user-data stream and adapter stop cleanly. (If you want a
   hard exit, press Ctrl-C twice — uvicorn will SIGKILL itself.)

## Useful launcher flags

```bash
bash launcher/start.sh --setup           # install only, no server
bash launcher/start.sh --no-browser      # do not auto-open browser
bash launcher/start.sh --host 0.0.0.0    # bind on every interface
bash launcher/start.sh --port 9000       # use a different port
bash launcher/start.sh --no-prompt       # abort if .env is missing
bash launcher/start.sh --reset           # wipe .venv + heatmap-sdk
                                         # (keeps .env)
```

`start.bat` accepts the same flags.

## Going to mainnet

In `.env`, change:

```
UTA_TESTNET=0
BINANCE_API_KEY=<mainnet key>
BINANCE_API_SECRET=<mainnet secret>
```

…or delete `.env` and re-run the launcher to be prompted again.

> Walk the testnet path end-to-end **at least once** before pointing
> a mainnet account at this. All 554 unit tests use mocks; real
> Binance integration bugs (auth headers, listenKey rotation,
> symbol-filter rounding edge cases, WS reconnect under real network
> conditions, race between adaptive-signal frequency and UTA's
> submit-lock) only show up against the live exchange.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `[launcher] ERROR: Python 3.11+ not found on PATH.` | No Python or too old. | Install Python 3.11+ from <https://www.python.org/downloads/>, tick "Add to PATH" on Windows. |
| `BinanceAuthError` on connect | Wrong key/secret, or testnet key used against mainnet (or vice-versa). | Verify `BINANCE_API_KEY` / `BINANCE_API_SECRET` in `.env` and that `UTA_TESTNET` matches the key's environment. |
| `One-way position mode required` | Binance account is in Hedge mode. | Switch to one-way mode in the Binance UI (Futures → Preferences → Position Mode). |
| `Task was destroyed but it is pending!` warnings on Ctrl-C | Server didn't get a chance to drain. | Press Ctrl-C once and wait up to ~10 s; the launcher forwards SIGINT and uvicorn tears down the user-data stream + adapter cleanly. |
| `patch ... corrupt patch` | Custom edits in `./heatmap-sdk/` conflict with the bundled patch. | Run `launcher/start.sh --reset` to wipe `./heatmap-sdk/` and let the launcher re-extract a clean copy. |
| Port 8000 already in use | Something else is bound on the port. | Pass `--port 9000` (or any free port) to the launcher. |

## What the launcher does NOT do

- **Close open positions on stop.** Toggling autotrade off prevents
  new entries but leaves running positions intact. Close them
  manually in the UI or in Binance.
- **Auto-update.** It does not `git pull` or refresh dependencies.
  When you `git pull`, re-run the launcher — pip is a no-op if all
  packages are already at the right versions, otherwise it installs
  what's missing.
- **Run on Python 3.10 or older.** The check is strict because UTA
  uses 3.11 syntax (union types, `Self`, etc.).
