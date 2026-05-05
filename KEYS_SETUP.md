# API Key Setup

**Created by Oscar Zarraga Perez. Copyright © 2026 Oscar Zarraga Perez.**

The terminal works with **zero API keys**. Every key listed here is
optional. Adding a key only adds an extra fallback layer to the rotation,
which makes the affected feature more resilient to rate-limits.

---

## Free providers (all optional)

| Provider                | Free quota          | Sign up                                                                      |
| ----------------------- | ------------------- | ---------------------------------------------------------------------------- |
| Alpha Vantage           | 5/min · 500/day     | <https://www.alphavantage.co/support/#api-key>                               |
| Financial Modeling Prep | 250/day             | <https://site.financialmodelingprep.com/developer>                           |
| Finnhub                 | 60/min              | <https://finnhub.io/register>                                                |
| Twelve Data             | 8/min · 800/day     | <https://twelvedata.com/pricing>                                             |

Each provider gives you the key immediately after a free email signup. No
credit card required for any of the four free tiers.

---

## Method A — `keys.json` file

This is the easiest method.

1. In the same folder as `server.py`, copy `keys.sample.json` to
   `keys.json`.
2. Open `keys.json` in any text editor.
3. Paste each key value between the quotes for that provider. Leave blank
   the ones you do not have.
4. Save and close.
5. Restart `server.py`.

Example `keys.json`:

```json
{
  "alpha_vantage": "PASTE_YOUR_KEY_HERE",
  "fmp":           "",
  "finnhub":       "",
  "twelve_data":   ""
}
```

The status bar in the bottom-right of the UI shows which keys are loaded
(green = active, grey = missing).

> `keys.json` is in `.gitignore` and will never be committed if this
> project is pushed to GitHub. Keep it that way.

---

## Method B — environment variables

If you prefer not to keep keys in a file, export them as environment
variables before launching `server.py`.

The variable names the server looks for are:

| Provider                | Variable name           |
| ----------------------- | ----------------------- |
| Alpha Vantage           | `ALPHA_VANTAGE_API_KEY` |
| Financial Modeling Prep | `FMP_API_KEY`           |
| Finnhub                 | `FINNHUB_API_KEY`       |
| Twelve Data             | `TWELVE_DATA_API_KEY`   |

**Windows (Command Prompt):**

```bat
set ALPHA_VANTAGE_API_KEY=your_key_here
python server.py
```

**Windows (PowerShell):**

```powershell
$env:ALPHA_VANTAGE_API_KEY = "your_key_here"
python server.py
```

**Mac/Linux:**

```sh
export ALPHA_VANTAGE_API_KEY="your_key_here"
python3 server.py
```

If you set both an env var and a value in `keys.json`, the env var wins.

---

## Verifying the key is loaded

After restarting the server, open the terminal and look at the bottom-right
status bar. Each provider's chip is green when its key is loaded and grey
when it is missing or invalid.

You can also hit the status endpoint directly:

```sh
curl http://127.0.0.1:8788/api/keys/status
```

The response is a JSON object listing each provider and a boolean for
whether a key is currently loaded.

---

## Security reminders

- Treat keys as secrets. The free tiers are still attached to your email
  address.
- Never paste keys into chat tools, public Pastebin, or screenshots.
- If you accidentally commit a key, revoke it on the provider's dashboard
  and generate a new one. Do not just delete the commit.
- Run `gitleaks`, `trufflehog`, or `detect-secrets` as a pre-commit hook if
  you fork this project.
