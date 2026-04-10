# Scalable US Health Leads Scraper

An advanced, highly concurrent web scraper, OSINT pipeline, and LLM-powered extraction engine designed to harvest high-intent health leads from platforms like Reddit and Twitter (X). The system leverages cutting-edge LLMs (via Ollama) to parse unstructured medical context, identifies user profiles, and performs parallel Bio-OSINT enrichment to extract real names, telephone numbers, and locations. 

---

## 🚀 Key Features

*   **Multi-Platform Target Acquisition:** Extracts unstructured health-related conversations from Reddit (Pushshift/Direct) and Twitter (via Nitter Stealth Mirrors).
*   **LLM Extraction Brain (Local Ollama):** Uses `llama3.2:3b` to read thread context, recognize high-intent disease signals (e.g., Chronic Lyme, Bartonella), and isolate PII structures.
*   **Parallel OSINT Enrichment:** Automatically identifies subtweet authors or thread respondents. Runs asynchronous deep-dive scans (`BioExtractor`, `Blackbird`, `WhatsMyName`) to cross-reference usernames against 500+ social platforms to find real-world identities and phone numbers.
*   **Stealth & Resilience:** 
    *   Bypasses Twitter login walls and rate limits using dynamic `nitter` network routing.
    *   Incorporates strict concurrency limiters (Semaphores) and proxy pool rotation to avoid IP bans.
*   **Format-Preserving Excel Sync:** Persists exported lead data into Excel (`.xlsx`) via `openpyxl`, actively preserving manual formatting, column widths, and customized styles configured by the user.

## 🛠️ Architecture

*   **Celery / Redis:** Manages the task distribution queue for bulk asynchronous ingestion and processing.
*   **PostgreSQL & SQLAlchemy:** Serves as the primary structured relational warehouse for validated leads.
*   **MongoDB:** Functions as the massive, unstructured Data Lake for raw scraped HTML/blobs before LLM-parsing.
*   **Playwright:** Operates headless browsers for complex dynamic JS-rendered targets when stealth API routing isn't applicable.

## ⚙️ Setup & Deployment

### Dependencies
Ensure you have Python 3.11+ installed.
Install project dependencies using Poetry:
```bash
poetry install
```

### Environment Variables
Duplicate `.env.example` to `.env` and fill in the required details:
```bash
cp .env.example .env
```
*(Ensure you supply a clean `DATABASE_URL` with your local Postgres credentials).*

### Runtime Infrastructure Requirements
- **Redis Server** (required for Celery worker queues)
- **PostgreSQL Database** (schema matching `health_leads`)
- **MongoDB** (for Raw Scrape hoarding)
- **Ollama** (running locally on port `11434` with the `llama3.2:3b` model pulled)

### Execution
1. **Launch Workers**: Ensure Celery is actively consuming the queues.
2. **Launch Acquisition**: Run ingestion scripts (e.g., `twitter_x_discover` / Reddit ingestion).
3. **Parse & Enrich**: Execute the main LLM processing loop to harvest and enrich raw MongoDB blobs.
4. **Excel Export**: Execute `python src/scripts/export_to_excel.py` to synchronize PostgreSQL data directly into a styled Excel tracker.

## 🔒 Security Posture

This project follows strict security guidelines:
*   **SQL Injection:** Obsoleted via robust `SQLAlchemy` ORM parameterization.
*   **Secret Management:** No API keys are hardcoded in the source code. All configuration connects securely via ignored local `.env` variables.
*   **Data Validation:** Incoming HTML blobs / web data is never executed. Payload parsing is securely structured using `Pydantic` schema verification before touching relational databases.
*   **Data Compliance:** Extracted data exports (CSV/Excel) containing harvested PII are explicitly excluded from Version Control via `.gitignore`.
