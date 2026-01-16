import os
import re
import time
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request, Header, HTTPException
from pydantic import BaseModel

# ----------------------------
# Config
# ----------------------------
API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")  # optional (but your code expects it)
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")  # required
TIMEOUT = 18

GOOGLE_SLEEP = 0.20
SITE_SLEEP = 0.35
MAX_SITE_PAGES = 4
MAX_OFFER_LEN = 30

CTA_PATTERNS = [
    r"\bcall\b", r"\btext\b", r"\bemail\b", r"\bmessage\b", r"\bcontact\b",
    r"\bbook\b", r"\bschedule\b", r"\bappointment\b", r"\bget a quote\b", r"\bget quote\b",
    r"\brequest\b", r"\bclick\b", r"\btap\b", r"\bvisit\b", r"\border\b", r"\bbuy\b",
    r"\bapply now\b", r"\bchat\b", r"\binquire\b", r"\bquote\b"
]
PHONE_REGEX = re.compile(r"(\+?1?\s*)?(\(?\d{3}\)?[\s\-\.]?\d{3}[\s\-\.]?\d{4})")
URL_REGEX = re.compile(r"(https?://\S+|www\.\S+)", re.IGNORECASE)

CANDIDATE_PATHS = ["", "specials", "offers", "deals", "coupons", "promotions", "financing", "warranty", "about", "services", "service", "contact"]

OFFER_PATTERNS = [
    r"\b\d{1,2}%\s*off\b", r"\bdiscount\b", r"\bcoupon\b", r"\bpromo\b", r"\bpromotion\b",
    r"\bspecial\b", r"\bdeal\b", r"\bsave\b", r"\bfree\b",
    r"\bno money down\b", r"\bstarting at\b", r"\b\$\s*\d+\b",
    r"\b0%\b", r"\bapr\b", r"\binterest\b", r"\bwarranty\b"
]
FINANCING_PATTERNS = [r"\bfinancing\b", r"\bmonthly payments\b", r"\b0% interest\b", r"\bno interest\b", r"\bdeferred\b", r"\bpre-qualify\b", r"\bcredit\b"]
WARRANTY_PATTERNS = [r"\bwarranty\b", r"\blifetime\b", r"\bguarantee\b", r"\bworkmanship\b"]
SPANISH_PATTERNS = [r"\bse habla espa(n|ñ)ol\b", r"\bhablamos espa(n|ñ)ol\b", r"\bespa(n|ñ)ol\b"]
INSURED_PATTERNS = [r"\binsured\b", r"\bfully insured\b", r"\bliability insured\b"]
BONDED_PATTERNS = [r"\bbonded\b", r"\blicensed and bonded\b"]
VIRTUAL_PATTERNS = [r"\bvirtual\b", r"\bvideo consultation\b", r"\bvirtual estimate\b", r"\bremote\b"]
MEMBERSHIP_PATTERNS = [r"\bbb\b", r"\bangi\b", r"\bhomeadvisor\b", r"\bnari\b", r"\bnahb\b", r"\bnate\b", r"\bchamber of commerce\b"]
CERT_PATTERNS = [r"\bcertified\b", r"\blicensed\b", r"\bepa\b", r"\bnate\b"]
AWARD_PATTERNS = [r"\baward\b", r"\bbest of\b", r"\btop rated\b", r"\bwinner\b"]
COMMUNITY_PATTERNS = [r"\bcommunity\b", r"\bdonate\b", r"\bsponsor\b", r"\bcharity\b", r"\bvolunteer\b"]

app = FastAPI()
session = requests.Session()

# ----------------------------
# Helpers (adapted from your script)
# ----------------------------
def clean_str(x) -> str:
    return "" if x is None else str(x).strip()

def normalize_phone(phone: str) -> str:
    digits = re.sub(r"\D+", "", clean_str(phone))
    if len(digits) == 10:
        return "+1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    return digits

def normalize_url(url: str) -> str:
    url = clean_str(url)
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url

def http_get(url: str, timeout: int = TIMEOUT):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    }
    return session.get(url, headers=headers, timeout=timeout, allow_redirects=True)

def extract_visible_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.decompose()
    text = soup.get_text(separator=" ")
    text = re.sub(r"\s+", " ", text).strip()
    return text

def get_meta(soup, name=None, prop=None) -> str:
    if name:
        tag = soup.find("meta", attrs={"name": name})
        if tag and tag.get("content"):
            return tag["content"].strip()
    if prop:
        tag = soup.find("meta", attrs={"property": prop})
        if tag and tag.get("content"):
            return tag["content"].strip()
    return ""

def strip_contact_and_cta(text: str) -> str:
    t = clean_str(text)
    if not t:
        return ""
    t = URL_REGEX.sub("", t)
    t = PHONE_REGEX.sub("", t)
    for pat in CTA_PATTERNS:
        t = re.sub(pat, "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"[\|\•]+", " ", t).strip()
    return t

def compress_to_offer(text: str) -> str:
    t = strip_contact_and_cta(text)
    if not t:
        return ""
    patterns = [
        r"(\b\d{1,2}%\s*off\b)",
        r"(\b\$?\s*\d+\s*(?:off|discount)\b)",
        r"(\bfree\s+\w+(?:\s+\w+)?\b)",
        r"(\b0%\s*(?:apr|interest)\b)",
        r"(\bfinancing\s+available\b)",
        r"(\bno\s+money\s+down\b)",
        r"(\blifetime\s+warranty\b)",
        r"(\b\d+\s*(?:year|yr)\s*warranty\b)",
        r"(\bsame[-\s]*day\s+service\b)",
        r"(\bprice\s*match\b)",
        r"(\bseasonal\s+special\b)",
    ]
    for p in patterns:
        m = re.search(p, t, flags=re.IGNORECASE)
        if m:
            t = m.group(1)

    t = re.sub(r"\s+", " ", t).strip()
    if len(t) > MAX_OFFER_LEN:
        t = t[:MAX_OFFER_LEN].rstrip()
        t = re.sub(r"\s+\S*$", "", t).strip()
        if not t:
            t = strip_contact_and_cta(text)[:MAX_OFFER_LEN].strip()

    if URL_REGEX.search(t) or PHONE_REGEX.search(t):
        return ""
    return t

def find_sentences(text: str, patterns: list[str], max_items: int = 3) -> list[str]:
    if not text:
        return []
    sentences = re.split(r"(?<=[.!?])\s+", text)
    rx = [re.compile(p, re.IGNORECASE) for p in patterns]
    hits = []
    for s in sentences:
        s = s.strip()
        if len(s) < 18:
            continue
        if any(r.search(s) for r in rx):
            s = strip_contact_and_cta(s)
            if s:
                hits.append(s[:240])
        if len(hits) >= max_items:
            break
    return hits

def scrape_site_bundle(website: str) -> dict:
    website = normalize_url(website)
    if not website:
        return {"status": "missing"}

    texts = []
    meta_description = ""
    og_description = ""
    title = ""
    pages_checked = []

    for path in CANDIDATE_PATHS:
        if len(pages_checked) >= MAX_SITE_PAGES:
            break
        try:
            url = urljoin(website.rstrip("/") + "/", path)
            resp = http_get(url, timeout=TIMEOUT)
            if resp.status_code >= 400:
                continue
            html = resp.text
            soup = BeautifulSoup(html, "lxml")
            if not meta_description:
                meta_description = get_meta(soup, name="description")
            if not og_description:
                og_description = get_meta(soup, prop="og:description")
            if not title and soup.title and soup.title.get_text():
                title = soup.title.get_text().strip()[:120]

            text = extract_visible_text(html)
            if len(text) < 200:
                continue

            texts.append(text)
            pages_checked.append(url)
            time.sleep(SITE_SLEEP)
        except Exception:
            continue

    if not texts:
        return {"status": "unreachable"}

    combined = " ".join(texts)

    offers_raw = find_sentences(combined, OFFER_PATTERNS, max_items=10)
    financing_raw = find_sentences(combined, FINANCING_PATTERNS, max_items=5)
    warranty_raw = find_sentences(combined, WARRANTY_PATTERNS, max_items=3)
    spanish = find_sentences(combined, SPANISH_PATTERNS, max_items=1)
    insured = find_sentences(combined, INSURED_PATTERNS, max_items=1)
    bonded = find_sentences(combined, BONDED_PATTERNS, max_items=1)
    virtual = find_sentences(combined, VIRTUAL_PATTERNS, max_items=1)

    offers = []
    for s in offers_raw:
        o = compress_to_offer(s)
        if o and o not in offers:
            offers.append(o)
        if len(offers) >= 3:
            break

    highlight_candidates = []
    for s in (offers_raw[:3] + financing_raw[:2] + warranty_raw[:2]):
        o = compress_to_offer(s)
        if o and o not in highlight_candidates:
            highlight_candidates.append(o)
        if len(highlight_candidates) >= 3:
            break

    highlights_tagline = strip_contact_and_cta(title) if title else strip_contact_and_cta(meta_description or og_description)
    highlights_tagline = highlights_tagline[:140] if highlights_tagline else ""

    financing_options = "; ".join([strip_contact_and_cta(x) for x in financing_raw[:2] if strip_contact_and_cta(x)])[:500]
    warranty_text = "; ".join([strip_contact_and_cta(x) for x in warranty_raw[:2] if strip_contact_and_cta(x)])[:500]

    return {
        "status": "ok",
        "pagesChecked": pages_checked,
        "metaDescription": strip_contact_and_cta(meta_description)[:300] if meta_description else "",
        "ogDescription": strip_contact_and_cta(og_description)[:300] if og_description else "",
        "title": strip_contact_and_cta(title) if title else "",
        "offers": offers[:3],
        "financingOptions": financing_options,
        "warrantyText": warranty_text,
        "spanish": bool(spanish),
        "insured": bool(insured),
        "bonded": bool(bonded),
        "virtual": bool(virtual),
        "highlights": highlight_candidates[:3],
        "highlightsTagline": highlights_tagline,
    }

# ----------------------------
# Google Places (same API endpoints you used)
# ----------------------------
def places_text_search(query: str) -> str | None:
    if not API_KEY:
        return None
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    r = session.get(url, params={"query": query, "key": API_KEY}, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    results = data.get("results", [])
    if not results:
        return None
    return results[0].get("place_id")

def places_details(place_id: str) -> dict:
    if not API_KEY or not place_id:
        return {}
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    fields = ",".join([
        "name","formatted_address","formatted_phone_number","website","rating","user_ratings_total",
        "reviews","opening_hours","editorial_summary","business_status","url"
    ])
    r = session.get(url, params={"place_id": place_id, "fields": fields, "key": API_KEY}, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json().get("result", {}) or {}

def format_top_reviews(reviews: list[dict], top_n: int = 5) -> str:
    if not reviews:
        return ""
    try:
        reviews = sorted(reviews, key=lambda x: x.get("time", 0), reverse=True)
    except Exception:
        pass

    out = []
    for rv in reviews[:top_n]:
        author = clean_str(rv.get("author_name"))
        rating = rv.get("rating")
        text = clean_str(rv.get("text"))
        if not text:
            continue
        text = strip_contact_and_cta(text)
        text = re.sub(r"\s+", " ", text)[:300]
        out.append(f"{author} ({rating}★): {text}")
    return "\n".join(out)

# ----------------------------
# Input model (from Gravity Forms)
# ----------------------------
class GravityPayload(BaseModel):
    firstName: str | None = ""
    lastName: str | None = ""
    companyName: str | None = ""
    phone: str | None = ""
    email: str | None = ""
    role: str | None = ""
    primaryTrade: str | None = ""
    leadsPerWeek: str | None = ""
    website: str | None = ""

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/webhook/gravityforms")
async def gravityforms_webhook(
    payload: GravityPayload,
    x_webhook_secret: str | None = Header(default=None),
):
    # Basic auth guard
    if not WEBHOOK_SECRET:
        raise HTTPException(status_code=500, detail="WEBHOOK_SECRET not set")
    if x_webhook_secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    company = clean_str(payload.companyName)
    website = normalize_url(payload.website or "")
    phone = normalize_phone(payload.phone or "")

    # Build a query for Places
    query_parts = [company]
    if phone:
        query_parts.append(phone)
    query = " ".join([p for p in query_parts if p]).strip()

    enriched: dict = {
        "input": payload.model_dump(),
        "normalized": {"website": website, "phone": phone},
        "google": {},
        "site": {},
        "output": {},
    }

    # Google enrichment
    if company and API_KEY:
        try:
            place_id = places_text_search(query)
            time.sleep(GOOGLE_SLEEP)
            details = places_details(place_id) if place_id else {}
            time.sleep(GOOGLE_SLEEP)

            if details:
                enriched["google"] = {
                    "rating": details.get("rating"),
                    "reviewCount": details.get("user_ratings_total"),
                    "website": details.get("website"),
                    "top5GoogleReviews": format_top_reviews(details.get("reviews", []), 5),
                    "businessHours": " | ".join((details.get("opening_hours", {}) or {}).get("weekday_text", []) or [])[:500],
                    "description": (details.get("editorial_summary", {}) or {}).get("overview", ""),
                    "mapsUrl": details.get("url"),
                }
        except Exception as e:
            enriched["google"] = {"error": str(e)}

    # Website scrape enrichment
    if website:
        try:
            enriched["site"] = scrape_site_bundle(website)
        except Exception as e:
            enriched["site"] = {"status": "error", "error": str(e)}

    # Build final outputs (your fields)
    g = enriched.get("google") or {}
    s = enriched.get("site") or {}
    offers = (s.get("offers") or []) if isinstance(s, dict) else []

    enriched["output"] = {
        "companyWebsite": g.get("website") or website,
        "averageGoogleReviewRating": g.get("rating") or "",
        "numberOfGoogleReviews": g.get("reviewCount") or "",
        "top5GoogleReviews": g.get("top5GoogleReviews") or "",
        "businessHours": strip_contact_and_cta(g.get("businessHours") or "")[:500],
        "description": strip_contact_and_cta((g.get("description") or (s.get("metaDescription") or "") or (s.get("ogDescription") or "") or ""))[:500] if isinstance(s, dict) else "",
        "specialOffer1": offers[0] if len(offers) > 0 else "",
        "specialOffer2": offers[1] if len(offers) > 1 else "",
        "specialOffer3": offers[2] if len(offers) > 2 else "",
        "offersFinancing": "yes" if (s.get("financingOptions") if isinstance(s, dict) else "") else "no",
        "financingOptions": ((s.get("financingOptions") or "") if isinstance(s, dict) else "")[:500],
        "waranty": ((s.get("warrantyText") or "") if isinstance(s, dict) else "")[:500],
        "highlightsTagline": ((s.get("highlightsTagline") or "") if isinstance(s, dict) else "")[:140],
        "isInsured": "yes" if (s.get("insured") if isinstance(s, dict) else False) else "no",
        "isBonded": "yes" if (s.get("bonded") if isinstance(s, dict) else False) else "no",
        "offersVirtualPresentations": "yes" if (s.get("virtual") if isinstance(s, dict) else False) else "no",
        "highlightPoint1": (s.get("highlights") or ["","",""])[0] if isinstance(s, dict) and (s.get("highlights") or []) else "",
        "highlightPoint2": (s.get("highlights") or ["","",""])[1] if isinstance(s, dict) and len(s.get("highlights") or []) > 1 else "",
        "highlightPoint3": (s.get("highlights") or ["","",""])[2] if isinstance(s, dict) and len(s.get("highlights") or []) > 2 else "",
        "highlightsTagline": (s.get("highlightsTagline") if isinstance(s, dict) else "")[:140],
    }

    # For now: return enriched JSON so you can see it works.
    # Later: write to Sheets/Airtable/DB here.
    return enriched

