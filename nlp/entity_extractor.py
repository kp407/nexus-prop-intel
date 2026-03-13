import spacy

nlp = spacy.load("en_core_web_sm")

MUMBAI_LOCATIONS = [
    "Mumbai", "Navi Mumbai", "BKC", "Bandra Kurla Complex",
    "Lower Parel", "Andheri", "Powai", "Thane", "Belapur",
    "Airoli", "Bhiwandi", "JNPT", "Nhava Sheva", "MIDC",
    "Worli", "Nariman Point", "Vikhroli", "Goregaon", "Malad"
]

def extract_entities(text: str) -> dict:
    doc = nlp(text[:100000])
    companies = list({ent.text for ent in doc.ents if ent.label_ == "ORG"})
    locations = list({ent.text for ent in doc.ents if ent.label_ in ("GPE", "LOC")})
    mumbai_hit = True  # Pan-India scope
    return {
        "companies": companies,
        "locations": locations,
        "mumbai_hit": mumbai_hit
    }
