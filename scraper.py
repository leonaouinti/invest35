"""
Invest35 — Scraper Immobilier Ille-et-Vilaine (v2)
===================================================
Sources:
  - DVF (data.gouv.fr)  — données officielles transactions
  - Logic-Immo          — annonces accessibles
  - Century21           — annonces accessibles
  - Laforêt             — annonces accessibles

INSTALLATION:
    pip3 install requests beautifulsoup4 lxml

UTILISATION:
    python3 scraper.py
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import random
import logging
import re
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────
MAX_PAGES = 3
DELAY_MIN = 3.0
DELAY_MAX = 7.0
OUTPUT_FILE = "properties.json"
GMKEY = "AIzaSyATL88HE9Dt3IjV8Zuzn3ARu7FgLboYYZ0"

logging.basicConfig(
    filename="scraper.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

HEADERS_LIST = [
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    },
]

SESSION = requests.Session()

def get_headers():
    return random.choice(HEADERS_LIST)

def sleep():
    t = random.uniform(DELAY_MIN, DELAY_MAX)
    print(f"    ⏳ Attente {t:.1f}s...")
    time.sleep(t)

def safe_get(url, retries=2, timeout=20):
    for attempt in range(retries):
        try:
            r = SESSION.get(url, headers=get_headers(), timeout=timeout)
            logging.info(f"GET {r.status_code} {url}")
            if r.status_code == 200:
                return r
            elif r.status_code in [403, 429]:
                print(f"    ⚠ Bloqué ({r.status_code})")
                time.sleep(10 * (attempt + 1))
            else:
                logging.warning(f"HTTP {r.status_code} for {url}")
        except Exception as e:
            logging.error(f"Error: {e} — {url}")
            time.sleep(5)
    return None

# ─── HELPERS ──────────────────────────────────────────────────────────────────
def clean_price(text):
    if not text: return None
    nums = re.sub(r"[^\d]", "", str(text))
    return int(nums) if nums and 4 <= len(nums) <= 9 else None

def clean_surface(text):
    if not text: return None
    m = re.search(r"(\d+)\s*m", str(text), re.IGNORECASE)
    return int(m.group(1)) if m else None

def clean_rooms(text):
    if not text: return 0
    m = re.search(r"(\d+)\s*(p|pièce|T(?=\d))", str(text), re.IGNORECASE)
    return int(m.group(1)) if m else 0

def infer_type(title):
    t = str(title).lower()
    if "immeuble" in t: return "Immeuble"
    if any(x in t for x in ["maison", "villa", "pavillon"]): return "Maison"
    if any(x in t for x in ["local", "commerce", "bureau"]): return "Local"
    if "terrain" in t: return "Terrain"
    return "Appartement"

def estimate_yield(price, surface, commune):
    if not price or not surface or price == 0: return 0.0
    rent_map = {
        "rennes": 13.5, "saint-malo": 14.0, "dinard": 13.0,
        "cesson": 12.5, "bruz": 12.0, "fougères": 9.5,
        "vitré": 10.0, "redon": 9.5, "liffré": 11.0
    }
    cl = str(commune).lower()
    rpm = next((v for k, v in rent_map.items() if k in cl), 11.0)
    return round((surface * rpm * 12 / price) * 100, 1)

def estimate_score(price, surface, yield_pct, commune, type_bien):
    score = 50
    if yield_pct >= 9: score += 20
    elif yield_pct >= 7: score += 14
    elif yield_pct >= 5: score += 7
    elif yield_pct < 3: score -= 10
    if price and surface and surface > 0:
        pm2 = price / surface
        if pm2 < 1500: score += 15
        elif pm2 < 2500: score += 10
        elif pm2 < 3500: score += 5
        elif pm2 > 5500: score -= 10
    cl = str(commune).lower()
    if "rennes" in cl: score += 8
    elif "saint-malo" in cl: score += 6
    elif any(x in cl for x in ["dinard", "cesson", "bruz"]): score += 4
    if type_bien == "Immeuble": score += 10
    return max(10, min(100, score))

def get_opportunity(score):
    if score >= 85: return "hot"
    if score >= 72: return "good"
    return "normal"

def get_zone(commune):
    cl = str(commune).lower()
    if any(x in cl for x in ["rennes", "cesson", "bruz", "liffré", "chantepie", "pacé", "betton"]):
        return "rennes"
    if any(x in cl for x in ["saint-malo", "dinard", "cancale", "briac"]):
        return "cote"
    return "interior"

def street_view(lat, lng):
    if lat and lng:
        return f"https://maps.googleapis.com/maps/api/streetview?size=600x300&location={lat},{lng}&fov=90&pitch=10&key={GMKEY}"
    return ""

def geocode(commune):
    try:
        url = f"https://api-adresse.data.gouv.fr/search/?q={commune}&limit=1&type=municipality"
        r = requests.get(url, timeout=8)
        if r.status_code == 200:
            feats = r.json().get("features", [])
            if feats:
                c = feats[0]["geometry"]["coordinates"]
                return round(c[1], 4), round(c[0], 4)
    except: pass
    return None, None

def build_prop(pid, commune, title, price, surface, rooms, lat, lng, photo, source, link, dpe="D"):
    type_bien = infer_type(title)
    surface = surface or 55
    price_m2 = round(price / surface) if surface else 0
    yield_pct = estimate_yield(price, surface, commune)
    score = estimate_score(price, surface, yield_pct, commune, type_bien)
    if not photo and lat and lng:
        photo = street_view(lat, lng)
    return {
        "id": pid,
        "commune": commune.strip().title(),
        "quartier": "",
        "type": type_bien,
        "price": int(price),
        "surface": int(surface),
        "rooms": int(rooms or 0),
        "priceM2": price_m2,
        "marketAvg": price_m2,
        "yield": yield_pct,
        "rentEstimate": round(surface * 11.5),
        "lat": lat,
        "lng": lng,
        "score": score,
        "opportunity": get_opportunity(score),
        "zone": get_zone(commune),
        "strategy": ["location", "meuble"] if type_bien == "Appartement" else ["location"],
        "yearBuilt": 1975,
        "dpe": dpe,
        "tags": [type_bien, commune.strip().title()],
        "trend": [round(price_m2 * x) for x in [0.85, 0.88, 0.91, 0.94, 0.97, 1.0]],
        "scores": {
            "location": min(95, score + 5),
            "rendement": min(95, round(yield_pct * 10)),
            "marche": 75, "liquidite": 70, "travaux": 65
        },
        "description": str(title)[:200],
        "source": source,
        "daysAgo": random.randint(0, 14),
        "link": link,
        "photo": photo or ""
    }

# ─── SOURCE 1: DVF — Official French government data ──────────────────────────
def scrape_dvf():
    print("\n🏛  DVF — Données officielles (data.gouv.fr)...")
    results = []
    pid = 1000

    communes_35 = [
        ("35238", "Rennes"), ("35281", "Saint-Malo"), ("35049", "Bruz"),
        ("35051", "Cesson-Sévigné"), ("35113", "Fougères"), ("35360", "Vitré"),
        ("35236", "Redon"), ("35070", "Dinard"), ("35162", "Liffré"),
    ]

    for code, commune in communes_35:
        url = f"https://files.data.gouv.fr/geo-dvf/latest/csv/35/communes/{code}.csv"
        print(f"  📄 {commune}...")
        r = safe_get(url, timeout=30)
        if not r:
            sleep()
            continue

        lines = r.text.strip().split("\n")
        if len(lines) < 2:
            continue

        headers = [h.strip().strip('"') for h in lines[0].split(",")]
        print(f"    → {len(lines)-1} transactions")

        count = 0
        for line in reversed(lines[1:]):  # most recent first
            if count >= 20:
                break
            try:
                vals = line.split(",")
                if len(vals) < len(headers):
                    continue
                row = dict(zip(headers, vals))

                nature = row.get("nature_mutation", "")
                if "Vente" not in nature:
                    continue

                price = clean_price(row.get("valeur_fonciere", ""))
                surface_raw = row.get("surface_reelle_bati", "")
                surface = int(float(surface_raw)) if surface_raw.strip() else None
                type_local = row.get("type_local", "Appartement")
                rooms_raw = row.get("nombre_pieces_principales", "0").strip()
                rooms = int(rooms_raw) if rooms_raw.isdigit() else 0
                lat_raw = row.get("latitude", "").strip()
                lng_raw = row.get("longitude", "").strip()
                lat = float(lat_raw) if lat_raw else None
                lng = float(lng_raw) if lng_raw else None

                if not price or price < 30000 or price > 2000000:
                    continue
                if not surface or surface < 10:
                    continue

                type_bien = "Maison" if "maison" in type_local.lower() else "Appartement"
                photo = street_view(lat, lng) if lat and lng else ""
                title = f"{type_bien} {surface}m² à {commune}"

                prop = build_prop(pid, commune, title, price, surface,
                                  rooms, lat, lng, photo, "ouestimmo", "", "D")
                prop["tags"] = [type_bien, commune, "DVF Officiel"]
                results.append(prop)
                pid += 1
                count += 1

            except Exception as e:
                logging.error(f"DVF row: {e}")
                continue

        sleep()

    print(f"  ✅ DVF: {len(results)} transactions")
    return results


# ─── SOURCE 2: Logic-Immo ─────────────────────────────────────────────────────
def scrape_logic_immo():
    print("\n🔵 Logic-Immo...")
    results = []
    pid = 2000

    for page in range(1, MAX_PAGES + 1):
        url = f"https://www.logic-immo.com/vente-immobilier-ille-et-vilaine,400_35/{page}_1.htm"
        print(f"  Page {page}...")
        r = safe_get(url)
        if not r:
            sleep()
            continue

        soup = BeautifulSoup(r.text, "lxml")
        cards = soup.select("article, [class*='offer'], [class*='property'], [class*='listing']")
        cards = [c for c in cards if c.select_one("[class*='price'], [class*='prix']")]
        print(f"  → {len(cards)} annonces")

        for card in cards:
            try:
                title_el = card.select_one("h2, h3, [class*='title']")
                title = title_el.get_text(strip=True) if title_el else ""
                if not title: continue

                price_el = card.select_one("[class*='price'], [class*='prix']")
                price = clean_price(price_el.get_text() if price_el else "")
                if not price or price < 30000: continue

                surface_el = card.select_one("[class*='surface'], [class*='area']")
                surface = clean_surface(surface_el.get_text() if surface_el else title)

                loc_el = card.select_one("[class*='location'], [class*='city'], [class*='commune']")
                commune = re.sub(r"\d{5}", "", loc_el.get_text(strip=True) if loc_el else "").strip() or "Ille-et-Vilaine"

                img_el = card.select_one("img[src]")
                photo = img_el.get("src", "") if img_el else ""
                if photo and not photo.startswith("http"):
                    photo = "https://www.logic-immo.com" + photo

                link_el = card.select_one("a[href]")
                link = link_el["href"] if link_el else ""
                if link and not link.startswith("http"):
                    link = "https://www.logic-immo.com" + link

                lat, lng = geocode(commune)
                prop = build_prop(pid, commune, title, price, surface or 60,
                                  clean_rooms(title), lat, lng, photo, "bienici", link)
                results.append(prop)
                pid += 1
            except Exception as e:
                logging.error(f"Logic-Immo: {e}")
                continue

        sleep()

    print(f"  ✅ Logic-Immo: {len(results)} annonces")
    return results


# ─── SOURCE 3: Century21 ──────────────────────────────────────────────────────
def scrape_century21():
    print("\n🔴 Century21...")
    results = []
    pid = 3000

    for page in range(1, MAX_PAGES + 1):
        url = f"https://www.century21.fr/annonces/vente/departement-ille-et-vilaine/?page={page}"
        print(f"  Page {page}...")
        r = safe_get(url)
        if not r:
            sleep()
            continue

        soup = BeautifulSoup(r.text, "lxml")
        cards = soup.select("article, [class*='property'], [class*='card-product']")
        cards = [c for c in cards if c.select_one("[class*='price'], [class*='prix']")]
        print(f"  → {len(cards)} annonces")

        for card in cards:
            try:
                title_el = card.select_one("h2, h3, [class*='title'], [class*='type']")
                title = title_el.get_text(strip=True) if title_el else ""
                if not title or len(title) < 4: continue

                price_el = card.select_one("[class*='price'], [class*='prix']")
                price = clean_price(price_el.get_text() if price_el else "")
                if not price or price < 30000: continue

                surface_el = card.select_one("[class*='surface'], [class*='area'], [class*='size']")
                surface = clean_surface(surface_el.get_text() if surface_el else title)

                loc_el = card.select_one("[class*='city'], [class*='location'], [class*='commune']")
                commune = re.sub(r"\(.*?\)|\d{5}", "", loc_el.get_text(strip=True) if loc_el else "").strip() or "Ille-et-Vilaine"

                img_el = card.select_one("img[src], img[data-src]")
                photo = (img_el.get("src") or img_el.get("data-src", "")) if img_el else ""

                link_el = card.select_one("a[href]")
                link = link_el["href"] if link_el else ""
                if link and not link.startswith("http"):
                    link = "https://www.century21.fr" + link

                lat, lng = geocode(commune)
                prop = build_prop(pid, commune, title, price, surface or 60,
                                  clean_rooms(title), lat, lng, photo, "seloger", link)
                results.append(prop)
                pid += 1
            except Exception as e:
                logging.error(f"Century21: {e}")
                continue

        sleep()

    print(f"  ✅ Century21: {len(results)} annonces")
    return results


# ─── SOURCE 4: Laforêt ────────────────────────────────────────────────────────
def scrape_laforet():
    print("\n🟢 Laforêt...")
    results = []
    pid = 4000

    for page in range(1, MAX_PAGES + 1):
        url = f"https://www.laforet.com/immobilier/acheter/annonces/ille-et-vilaine-35/?p={page}"
        print(f"  Page {page}...")
        r = safe_get(url)
        if not r:
            sleep()
            continue

        soup = BeautifulSoup(r.text, "lxml")
        cards = soup.select("[class*='property'], [class*='listing'], article, [class*='card']")
        cards = [c for c in cards if c.select_one("[class*='price'], [class*='prix']")]
        print(f"  → {len(cards)} annonces")

        for card in cards:
            try:
                title_el = card.select_one("h2, h3, [class*='title'], [class*='type']")
                title = title_el.get_text(strip=True) if title_el else ""
                if not title or len(title) < 4: continue

                price_el = card.select_one("[class*='price'], [class*='prix']")
                price = clean_price(price_el.get_text() if price_el else "")
                if not price or price < 30000: continue

                surface_el = card.select_one("[class*='surface'], [class*='area']")
                surface = clean_surface(surface_el.get_text() if surface_el else title)

                loc_el = card.select_one("[class*='city'], [class*='location'], [class*='ville']")
                commune = re.sub(r"\(.*?\)|\d{5}", "", loc_el.get_text(strip=True) if loc_el else "").strip() or "Ille-et-Vilaine"

                img_el = card.select_one("img[src], img[data-src]")
                photo = (img_el.get("src") or img_el.get("data-src", "")) if img_el else ""

                link_el = card.select_one("a[href]")
                link = link_el["href"] if link_el else ""
                if link and not link.startswith("http"):
                    link = "https://www.laforet.com" + link

                lat, lng = geocode(commune)
                prop = build_prop(pid, commune, title, price, surface or 60,
                                  clean_rooms(title), lat, lng, photo, "leboncoin", link)
                results.append(prop)
                pid += 1
            except Exception as e:
                logging.error(f"Laforet: {e}")
                continue

        sleep()

    print(f"  ✅ Laforêt: {len(results)} annonces")
    return results


# ─── DEDUP & CLEAN ────────────────────────────────────────────────────────────
def deduplicate(props):
    seen = set()
    out = []
    for p in props:
        key = (int(p["price"] / 1000), p["commune"][:8], int(p.get("surface", 0) / 5))
        if key not in seen:
            seen.add(key)
            out.append(p)
    return out

def clean_all(props):
    for i, p in enumerate(props):
        p["id"] = i + 1
        p["yield"] = round(float(p.get("yield", 0)), 1)
        p["score"] = max(10, min(100, int(p.get("score", 50))))
        p["surface"] = max(10, int(p.get("surface", 50)))
        p["commune"] = p.get("commune") or "Ille-et-Vilaine"
    return sorted(props, key=lambda x: x["score"], reverse=True)


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  Invest35 Scraper v2")
    print("=" * 60)
    print(f"  {datetime.now().strftime('%d/%m/%Y %H:%M')}")

    all_props = []

    for scraper, name in [
        (scrape_dvf, "DVF"),
        (scrape_logic_immo, "Logic-Immo"),
        (scrape_century21, "Century21"),
        (scrape_laforet, "Laforêt"),
    ]:
        try:
            results = scraper()
            all_props.extend(results)
            print(f"  Running total: {len(all_props)}")
        except Exception as e:
            print(f"  ❌ {name}: {e}")
            logging.error(f"{name} failed: {e}")

    all_props = deduplicate(all_props)
    all_props = clean_all(all_props)

    print(f"\n📊 Total final: {len(all_props)} annonces")

    if not all_props:
        print("⚠ Aucune annonce. Vérifiez scraper.log")
        return

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump({"generated_at": datetime.now().isoformat(), "total": len(all_props), "properties": all_props},
                  f, ensure_ascii=False, indent=2)

    print(f"✅ Sauvegardé: {OUTPUT_FILE}")
    print(f"\n🏆 Top 5:")
    for p in all_props[:5]:
        print(f"  [{p['score']}/100] {p['commune']} — {p['type']} {p['surface']}m² — {p['price']:,}€ — {p['yield']}% rdt")

    print(f"\n▶ python3 inject.py")
    print("=" * 60)

if __name__ == "__main__":
    main()
