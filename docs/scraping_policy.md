# PakFuelWatch Web Scraping Policy & Architecture

This document outlines the scraping guidelines, operational standards, and architectural design for web scrapers in the PakFuelWatch pipeline (particularly the Pakistan Bureau of Statistics portal and related economic data sources).

---

## 1. Core Principles

1. **Polite, Ethical Ingestion**:
   - Every scraper must treat source servers with care. Scrapers must never overwhelm target websites or generate excessive traffic.
   - Operations are scheduled strictly at the frequency required by release cadences (e.g. weekly for SPI, monthly for CPI).

2. **Robots.txt Adherence**:
   - All browser navigations and HTTP calls check the domain's `robots.txt` file prior to requesting page resources.
   - If a target URL path matches a `Disallow` rule for web crawlers, navigation is halted with an explicit log warning.

3. **Rate Limiting**:
   - A mandatory minimum delay of **2.0 seconds** is enforced between consecutive requests to the same host.
   - Host request timestamps are tracked across requests to ensure compliance.

4. **Retries with Exponential Backoff**:
   - Navigation failures, timeouts, and transient connection drops are retried up to 3 times with exponential backoff and jitter.
   - Scrapers must never loop aggressively on hard 4xx client errors.

---

## 2. Browser Automation (Patchright) Architecture

Scraping dynamic portals (like `pbs.gov.pk`) utilizes **Patchright**, an undetected Playwright fork designed to bypass bot detection heuristics naturally:

- **Browser Context**:
  - Context is launched using `launch_persistent_context` with a persistent user data directory (`.patchright_user_data`). This preserves cookies, cache, and session tokens between invocations.
  - `no_viewport=True` is set to inherit real window dimensions.
- **Fingerprinting Protection**:
  - **Do NOT provide custom `User-Agent` strings or extra HTTP headers.**
  - Patchright automatically synchronizes browser fingerprint headers with the underlying browser binary. Injecting synthetic headers causes fingerprint mismatches that trigger anti-bot defenses (Cloudflare, Akamai, etc.).
- **Execution Environments**:
  - **Docker / Production**: Uses bundled Chromium installed via `patchright install --with-deps chromium`.
  - **Local Development**: Configurable via `BROWSER_CHANNEL="chrome"` if Google Chrome is installed locally, or fallback to bundled Chromium.
  - **Headless Mode**: Configurable via `BROWSER_HEADLESS` (defaults to `true`).

---

## 3. Data Storage, Sidecars, and Caching

Every browser interaction produces immutable raw Bronze artifacts:

1. **HTML Page Snapshots**:
   - Saved to `s3://bronze/{source}/{yyyy-mm-dd}/{filename}.html`.
   - Accompanied by a JSON metadata sidecar (`source_url`, `fetched_at` UTC, `sha256`, `http_status`, `content_type`).

2. **Full-Page Screenshots**:
   - Captured as PNG at the time of scraping and stored to `s3://bronze/{source}/{yyyy-mm-dd}/{filename}.png`.
   - Provides visual auditing and historical verification if the target website layout changes.

3. **Downloaded Release Files (PDF / Excel)**:
   - Downloaded directly into Bronze `s3://bronze/{source}/{yyyy-mm-dd}/{filename}`.

4. **SHA-256 Deduplication**:
   - Bronze storage checks file content SHA-256 hashes before writing. If a file with the identical hash already exists for that source and date partition, writing is skipped.

---

## 4. Centralized Selectors (`selectors.yaml`)

To ensure maintainability as target websites redesign their DOM:
- All CSS and XPath selectors live in [`ingestion/selectors.yaml`](file:///home/tekglide/Documents/PakFuelWatch/ingestion/selectors.yaml).
- Python code imports selectors via `get_selector(source, key)` instead of embedding hardcoded strings.
- When a portal changes layout, only the YAML file needs updating—pipeline code and tests remain untouched.

---

## 5. Parse Failures and Quarantine

Data parsing operations on PDFs (`pdfplumber`) and Excel workbooks (`openpyxl`) must be resilient:
- Parsing errors (e.g. corrupt files, altered sheet layouts, unexpected column formats) are logged with:
  - Exact file name
  - Sheet name or PDF page number
  - Exception traceback
- Faulty or unparseable files are immediately moved to a quarantine path:
  `s3://silver/_quarantine/{source}/{filename}`
- Quarantining prevents bad files from crashing the pipeline, while alerting operators to inspect the schema deviation.
