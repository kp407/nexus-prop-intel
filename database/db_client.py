import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

def get_client() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    return create_client(url, key)

def upsert_company(client: Client, company_name: str, industry: str = None,
                   website: str = None, hq: str = None) -> str:
    from nlp.text_cleaner import normalize_company_name
    normalized = normalize_company_name(company_name)
    result = client.table("companies").upsert({
        "company_name": company_name,
        "normalized_name": normalized,
        "industry": industry,
        "website": website,
        "hq_location": hq
    }, on_conflict="normalized_name").execute()
    
    if result.data:
        return result.data[0]["company_id"]
    
    # If upsert returned empty, fetch the existing row
    existing = client.table("companies").select("company_id")\
        .eq("normalized_name", normalized).execute()
    return existing.data[0]["company_id"]

def insert_signal(client: Client, company_id: str, signal: dict) -> str:
    result = client.table("signals").insert({
        "company_id": company_id,
        "signal_type": signal["signal_type"],
        "space_type": signal.get("space_type"),
        "location": signal.get("location"),
        "confidence_score": signal["confidence"],
        "summary": signal.get("summary"),
        "source_url": signal.get("source_url")
    }).execute()
    return result.data[0]["signal_id"]

def upsert_lead_score(client: Client, company_id: str, score_data: dict):
    client.table("lead_scores").upsert({
        "company_id": company_id,
        **score_data
    }, on_conflict="company_id").execute()
