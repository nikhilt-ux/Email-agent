# Merch.AI — Merchandising Email Agent

> **Automatically reads your Gmail inbox, analyses vendor threads with GPT-4o, and writes structured data to Google Sheets — all from a beautiful Streamlit dashboard.**

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?style=flat-square&logo=python)
![Streamlit](https://img.shields.io/badge/Streamlit-1.x-FF4B4B?style=flat-square&logo=streamlit)
![OpenAI](https://img.shields.io/badge/OpenAI-GPT--4o-412991?style=flat-square&logo=openai)
![Google Sheets](https://img.shields.io/badge/Google%20Sheets-Synced-34A853?style=flat-square&logo=googlesheets)

---

## What It Does

The **Merchandising AI Agent** connects to a Gmail inbox, finds vendor email threads, and uses GPT-4o to automatically extract:

| Field | Description |
|---|---|
| **Division** | Product category (Women's Apparel, Footwear, etc.) |
| **Style No** | Style number from the email |
| **Colour** | Product colour mentions |
| **Intent** | Thread intent (Chase, Quality Issue, Approval, etc.) |
| **AI Overview** | 3–4 bullet summary of the thread in English |
| **Reply Needed** | Whether the latest message needs a response |
| **Reply Draft** | AI-generated reply suggestion |
| **Sample Status** | Dispatched / Received / Approved / Rejected / Pending |
| **AWB No** | Shipment tracking number (newest wins) |
| **Shipment Company** | Carrier name |
| **PO Number** | Purchase order number |
| **Attachments** | Detected attachments (PDF, images, etc.) |
| **Shared Links** | Google Drive, Dropbox, WeTransfer links |
| **Vendor Name** | Auto-matched from vendor database |

All data is written to a **Google Sheet** with separate tabs for Logs and Error Logs.

---

## Screenshots

| Dashboard | Thread Viewer | Sync |
|---|---|---|
| KPI metrics, chase alerts, charts, vendor breakdown | Browse/filter all threads with full AI details | Live sync progress with coloured log output |

---

## Architecture

```
Gmail API
    │
    ▼
gmail_reader.py ──► GPT-4o (OpenAI API)
    │                   │
    │              JSON analysis
    │                   │
    ▼                   ▼
Google Sheets ◄─── process_thread()
    │
    ▼
app.py (Streamlit dashboard)
```

**Key files:**

| File | Purpose |
|---|---|
| `app.py` | Streamlit frontend — Dashboard, Sync, Thread Viewer |
| `gmail_reader.py` | Core engine — Gmail auth, LLM calls, Sheets sync |
| `requirements.txt` | Python dependencies |
| `.streamlit/secrets.toml.template` | Secrets template for cloud deployment |

---

## Setup — Local Development

### 1. Clone the repo
```bash
git clone https://github.com/nikhilt-ux/Email-agent.git
cd Email-agent
```

### 2. Create a virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate       # macOS / Linux
.venv\Scripts\activate          # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Set up Google Cloud credentials

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project → Enable **Gmail API** and **Google Sheets API**
3. Create **OAuth 2.0 credentials** (Desktop app type)
4. Download the JSON and save as `credentials.json` in the project root

### 5. Add your OpenAI API key
Create a `.env` file:
```env
OPENAI_API_KEY=sk-proj-...
```

### 6. Configure Google Sheet ID
Edit `gmail_reader.py`:
```python
SHEET_ID = "your-google-sheet-id-here"
```

### 7. Run the app
```bash
# First run: opens browser for Gmail OAuth consent
.venv/bin/streamlit run app.py
```

A `token.json` will be created automatically after OAuth. Subsequent runs won't require login.

---

## Deployment — Railway / Render

The app supports cloud deployment via **environment variables** — no files needed.

### 1. Set `GOOGLE_TOKEN_JSON`

After authenticating locally (step 7 above), open your generated `token.json` and copy the full JSON contents. In your deployment platform dashboard, add:

```
Name:  GOOGLE_TOKEN_JSON
Value: {"token":"ya29...","refresh_token":"1//...","token_uri":"...","client_id":"...","client_secret":"...","scopes":[...],"expiry":"..."}
```

### 2. Set `OPENAI_API_KEY`
```
Name:  OPENAI_API_KEY
Value: sk-proj-...
```

### 3. Set start command
```bash
streamlit run app.py --server.port $PORT --server.address 0.0.0.0
```

### Streamlit Community Cloud

Use `st.secrets` instead. Add to your app's **Secrets** dashboard:
```toml
[google_token]
token         = "ya29...."
refresh_token = "1//..."
token_uri     = "https://oauth2.googleapis.com/token"
client_id     = "....apps.googleusercontent.com"
client_secret = "..."
scopes        = ["https://www.googleapis.com/auth/gmail.readonly", "https://www.googleapis.com/auth/spreadsheets"]
expiry        = "2025-01-01T00:00:00Z"
```

See [`.streamlit/secrets.toml.template`](.streamlit/secrets.toml.template) for the full template.

---

## Configuration

Key constants in `gmail_reader.py`:

```python
SHEET_ID         = "your-sheet-id"       # Google Sheet to write to
SHEET_TAB        = "Logs"                # Main log tab name
ERROR_TAB        = "Error Logs"          # Error log tab name
OPENAI_MODEL     = "gpt-4o"             # LLM model
OPENAI_TIMEOUT   = 60                    # Seconds per LLM call
GMAIL_WORKERS    = 5                     # Parallel thread fetchers
MAX_MSGS_IN_LLM  = 8                     # Max messages sent to LLM per thread
BODY_CHARS_PER_MSG = 3000               # Character limit per message
ONEQUINCE_DOMAIN = "@onequince.com"     # Your internal domain (filters out internal senders)
```

---

## How Shipment Extraction Works

The agent uses a **3-pass newest-first search** to always capture the most recent tracking info:

1. **Pass 1** — Scan from newest message → oldest. Return immediately if a message has both carrier + AWB
2. **Pass 2** — Any AWB found in newest messages (carrier may be in an older message)
3. **Pass 3** — Fallback: search full concatenated thread (carrier in one message, AWB in another)

---

## Google Sheet Schema

The Logs tab has **23 columns (A–W)**:

`Thread ID` · `Date Added` · `Sent Date` · `Last Updated` · `Sender` · `CC` · `Subject` · `Thread Messages` · `Division` · `Style No` · `Colour` · `Intent` · `AI Overview` · `Reply Needed` · `Reply Draft` · `Sample Status` · `Sample Reminder` · `AWB No` · `Shipment Company` · `Shipment Date` · `PO Number` · `Attachments` · `Shared Links`

---

## Security Notes

- **Never commit** `credentials.json`, `token.json`, or `.env` — all are in `.gitignore`
- Use environment variables or Streamlit secrets for all credentials in production
- The OpenAI API key is read from `OPENAI_API_KEY` env var (`.env` for local dev)
- The Google token refreshes automatically when expired

---

## Tech Stack

- **[Streamlit](https://streamlit.io)** — Frontend dashboard
- **[OpenAI GPT-4o](https://platform.openai.com)** — Thread analysis & reply drafts
- **[Gmail API](https://developers.google.com/gmail/api)** — Email reading
- **[Google Sheets API](https://developers.google.com/sheets/api)** — Data storage
- **[pandas](https://pandas.pydata.org)** — Data handling

---

## License

Internal tool — © One Quince. All rights reserved.
