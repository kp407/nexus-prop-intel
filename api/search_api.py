from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from database.db_client import get_client

app = FastAPI(title="Nexus Prop Intel API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

@app.get("/search")
def search(q: str = Query(..., description="Search query")):
    client = get_client()
    companies = client.table("companies").select("*").text_search(
        "fts", q, config="english"
    ).limit(20).execute()
    signals = client.table("signals").select(
        "*, companies(company_name)"
    ).text_search("fts", q, config="english").limit(20).execute()
    return {"companies": companies.data, "signals": signals.data}

@app.get("/company/{company_id}")
def get_dossier(company_id: str):
    client = get_client()
    company = client.table("companies").select("*").eq(
        "company_id", company_id
    ).single().execute()
    signals = client.table("signals").select("*, documents(*)").eq(
        "company_id", company_id
    ).order("timestamp", desc=True).execute()
    score = client.table("lead_scores").select("*").eq(
        "company_id", company_id
    ).single().execute()
    return {"company": company.data, "signals": signals.data, "lead_score": score.data}

@app.get("/feed")
def signal_feed(limit: int = 50, priority: str = None):
    client = get_client()
    query = client.table("signals").select(
        "*, companies(company_name, industry)"
    ).order("timestamp", desc=True)
    if priority:
        scores = client.table("lead_scores").select("company_id").eq(
            "priority_level", priority.upper()
        ).execute()
        ids = [r["company_id"] for r in scores.data]
        query = query.in_("company_id", ids)
    result = query.limit(limit).execute()
    return {"signals": result.data}
