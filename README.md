# NEXUS ASIA PROP INTEL

Autonomous CRE demand signal intelligence engine for Mumbai & Navi Mumbai.

## Stack
- Python (crawler, NLP, scoring, API)
- Supabase (PostgreSQL + full-text search)
- FastAPI (REST API)
- Static HTML/CSS/JS (frontend)
- GitHub Actions (free scheduler)

## Setup

### 1. Supabase
1. Create project at https://supabase.com
2. Run `database/schema.sql` in the SQL editor
3. Copy your project URL and service role key

### 2. Python Environment
```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

### 3. Configure Environment
```bash
cp .env.example .env
# Edit .env with your Supabase credentials
```

### 4. Run First Crawl
```bash
python main.py
```

### 5. Start API Server
```bash
uvicorn api.search_api:app --host 0.0.0.0 --port 8000
```

### 6. Start Scheduler (local)
```bash
python scheduler/cron_jobs.py
```

### 7. Frontend
- Edit `frontend/assets/app.js` — set `API_BASE` to your deployed API URL
- Host `/frontend` on GitHub Pages:
  - Push repo to GitHub
  - Settings > Pages > Deploy from `/frontend` folder

### 8. GitHub Actions (auto-scheduler, free)
- Add `SUPABASE_URL` and `SUPABASE_KEY` to GitHub repo Secrets
- The workflow at `.github/workflows/crawl.yml` runs every 6 hours automatically

## Folder Structure
```
nexus-prop-intel/
├── main.py
├── requirements.txt
├── .env.example
├── config/
│   ├── keywords.json
│   └── sources.json
├── crawler/
├── nlp/
├── scoring/
├── database/
├── api/
├── scheduler/
├── frontend/
└── .github/workflows/
```
