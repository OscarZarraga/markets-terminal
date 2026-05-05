# Terminal

A single-file market-data terminal. One Python backend + one HTML frontend.
No build step, no Node, no database, no third-party Python packages required —
just Python 3.

**Created by Oscar Zarraga Perez.** Copyright © 2026 Oscar Zarraga Perez.
Released under the [MIT License](LICENSE).

> **Status:** personal-use, educational. **No warranty. Not investment
> advice.** See [DISCLAIMER.md](DISCLAIMER.md) before running.

---

## What it does

- Live quotes, intraday and historical charts (multi-source, with automatic
  fallbacks).
- Fundamentals (income statement, balance sheet, cash flow) sourced primarily
  from SEC EDGAR XBRL.
- Earnings, peer companies, insider transactions, dividend history.
- Press releases aggregated from up to six feeds in parallel — including
  TDnet (Tokyo Stock Exchange) for foreign tickers.
- News from Google News, Yahoo Finance, GlobeNewswire, plus optional Finnhub
  and FMP feeds when keys are configured.
- StockTwits sentiment stream and bull/bear ratio.
- SEC filings (8-K, 10-K, 10-Q, 6-K) with direct EDGAR document links.

The full architecture, fallback chains, and caching/throttling rules are
documented in [`docs/Terminal_Architecture.pdf`](docs/Terminal_Architecture.pdf).

---

## Files in this bundle

| File                              | Purpose                                                  |
| --------------------------------- | -------------------------------------------------------- |
| `server.py`                       | The backend. Standard library only.                      |
| `terminal.html`                   | The frontend. Single HTML file.                          |
| `keys.sample.json`                | Template for API keys. Copy to `keys.json` and fill in.  |
| `KEYS_SETUP.md`                   | Detailed instructions for adding keys.                   |
| `RUN.bat` *or* `WINDOWS_LAUNCH.txt` | Windows launcher (text variant in the email zip).      |
| `run.sh`                          | Mac/Linux launcher. `chmod +x run.sh && ./run.sh`        |
| `docs/Terminal_Architecture.pdf`  | Full architecture & open-source security guide.          |
| `LICENSE`                         | MIT license — copyright Oscar Zarraga Perez.             |
| `DISCLAIMER.md`                   | Liability and no-warranty notice.                        |
| `.gitignore`                      | Keeps `keys.json` out of any git history.                |
| `README.md`                       | This file.                                               |

> **What is *not* in this bundle:** any real API key, any contact email, any
> internal IP address. The bundle is safe to redistribute as-is.

---

## Quick start

### 1. Requirements

- Python 3.9 or newer (`python3 --version` to check).
- A modern browser (Chrome, Firefox, Edge, Safari).
- That's it. No `pip install` needed for the basic run.

### 2. First run (zero keys)

The terminal works with **no API keys at all**. Public sources cover quotes,
charts, news, press releases, and SEC filings.

**Windows** — open Command Prompt (or PowerShell) in this folder and run:

```bat
python server.py
```

> If `RUN.bat` is included in this bundle, you can also just double-click it.
> If only `WINDOWS_LAUNCH.txt` is here, follow the instructions inside to
> create your own `RUN.bat` (email systems strip `.bat` attachments).

**Mac/Linux:**

```sh
chmod +x run.sh
./run.sh
```

Or directly, on any platform:

```sh
python3 server.py
```

The terminal opens automatically at <http://127.0.0.1:8788>.

### 3. (Optional) Add free API keys

Adding any of the four free-tier keys unlocks an additional fallback layer
in the rotation, makes price data more resilient to rate-limits, and adds
deeper fundamentals coverage. **Every key is optional — the terminal still
runs without them.**

| Provider                | Free quota          | Sign up                                                                      |
| ----------------------- | ------------------- | ---------------------------------------------------------------------------- |
| Alpha Vantage           | 5/min · 500/day     | <https://www.alphavantage.co/support/#api-key>                               |
| Financial Modeling Prep | 250/day             | <https://site.financialmodelingprep.com/developer>                           |
| Finnhub                 | 60/min              | <https://finnhub.io/register>                                                |
| Twelve Data             | 8/min · 800/day     | <https://twelvedata.com/pricing>                                             |

To install a key:

1. Copy `keys.sample.json` to `keys.json` (next to `server.py`).
2. Paste the key value between the quotes for that provider.
3. Restart `server.py`. The status bar in the bottom-right of the UI shows
   which keys are loaded (green = active, grey = missing).

Example `keys.json`:

```json
{
  "alpha_vantage": "PASTE_YOUR_KEY_HERE",
  "fmp":           "",
  "finnhub":       "",
  "twelve_data":   ""
}
```

You can also export keys as environment variables instead of editing the
file — see [`KEYS_SETUP.md`](KEYS_SETUP.md) for the variable names.

> **`keys.json` is in `.gitignore`** — it will never be committed if this
> repo is pushed to GitHub. Always keep it that way.

---

## How it stays resilient

Every quote and chart request fans out to multiple public sources in
parallel. If one returns a 429 ("too many requests") or 999 ("blocked"),
that host gets a 60-second cooldown and the chain advances to the next
source automatically. A single rate-limit never breaks a quote.

The full fallback chain is documented in
[`docs/Terminal_Architecture.pdf`](docs/Terminal_Architecture.pdf), Section 5.

---

## Public data sources used

All free, all public, all unkeyed:

- **Yahoo Finance** v7/v8 — <https://finance.yahoo.com>
- **Stooq** — <https://stooq.com>
- **CoinGecko** (crypto) — <https://www.coingecko.com>
- **SEC EDGAR** — <https://www.sec.gov/edgar.shtml>
- **Nasdaq Data API** — <https://api.nasdaq.com>
- **StockAnalysis** — <https://stockanalysis.com>
- **Finviz** — <https://finviz.com>
- **Cboe** — <https://www.cboe.com>
- **Google News** — <https://news.google.com>
- **GlobeNewswire** — <https://www.globenewswire.com>
- **StockTwits** — <https://stocktwits.com>
- **Yahoo Finance Japan** (TDnet) — <https://finance.yahoo.co.jp>
- **Kabutan** (TDnet) — <https://kabutan.jp>

Each source has its own terms of service. The operator is responsible for
reviewing and complying with the terms of every source they query. See the
PDF, Section 12 ("Licensing, Attribution & Legal Posture").

---

## Author & attribution

This project was created and authored by **Oscar Zarraga Perez**.

- Copyright © 2026 Oscar Zarraga Perez
- Released under the [MIT License](LICENSE)
- The MIT License **requires** that the copyright notice and this attribution
  be retained in all copies or substantial portions of the software, including
  in source-file headers, in the rendered application footer, and in any
  redistributions or derivative works.
- Removing or obscuring the author attribution is a breach of the MIT License
  under which this project is distributed.
- The application footer renders "Created by Oscar Zarraga Perez" at runtime.
  Every HTTP response from `server.py` carries an `X-Author: Oscar Zarraga
  Perez` header, and the `/api/about` endpoint returns the canonical
  authorship metadata as JSON.

When forking or redistributing, please preserve the author credit. If you
build something cool on top of this, a link back is appreciated but not
required.

---

## Security notes for forks and redeployments

If you fork this project:

1. **Never commit `keys.json`.** It is in `.gitignore` for a reason.
2. **Change the SEC `User-Agent`** in `server.py` to identify your own
   deployment. The default is a generic placeholder.
3. **Keep the bind address at `127.0.0.1`** unless you know what you are
   doing. The CORS `*` header is only safe because nothing on the network
   can reach the server.
4. **Run a secret-scanner** (`gitleaks`, `trufflehog`, or `detect-secrets`)
   as a pre-commit hook. Most credential leaks happen on the very first
   push and stay searchable on GitHub forever.
5. **Keep the author attribution.** It's a license requirement, not just
   etiquette.

The full security checklist is in
[`docs/Terminal_Architecture.pdf`](docs/Terminal_Architecture.pdf), Section 11.

---

## License

MIT — see [LICENSE](LICENSE). Copyright © 2026 Oscar Zarraga Perez.

## Disclaimer

The project is provided "AS IS", without warranty of any kind. Nothing in
this project, the documentation, or the source code constitutes investment,
legal, accounting, tax, or financial advice. Read [DISCLAIMER.md](DISCLAIMER.md)
in full before running.
