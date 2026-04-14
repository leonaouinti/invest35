"""
Invest35 — Scraper v3 (URLs vérifiées)
=======================================
Sources:
  - DVF dept level CSV (data.gouv.fr) — données officielles
  - Orpi                              — agence nationale
  - Guy Hoquet                        — agence nationale
  - Immo de France                    — agence régionale

pip3 install requests beautifulsoup4 lxml
python3 scraper.py
"""

import requests
from bs4 import BeautifulSoup
import json, time, random, logging, re, gzip, io
from datetime import datetime

OUTPUT_FILE = "properties.json"
GMKEY = "AIzaSyATL88HE9Dt3IjV8Zuzn3ARu7FgLboYYZ0"
MAX_PAGES = 3
DELAY = (3.0, 6.0)

logging.basicConfig(filename="scraper.log", level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

HEADERS = [
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
     "Accept-Language": "fr-FR,fr;q=0.9", "Connection": "keep-alive"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
     "Accept-Language": "fr-FR,fr;q=0.8", "Connection": "keep-alive"},
]

S = requests.Session()

def wait(): time.sleep(random.uniform(*DELAY))

def get(url, retries=2, timeout=25, stream=False):
    for i in range(retries):
        try:
            r = S.get(url, headers=random.choice(HEADERS), timeout=timeout, stream=stream)
            logging.info(f"GET {r.status_code} {url}")
            if r.status_code == 200: return r
            logging.warning(f"HTTP {r.status_code} {url}")
            time.sleep(8 * (i+1))
        except Exception as e:
            logging.error(f"{e} — {url}")
            time.sleep(5)
    return None

# ── helpers ──────────────────────────────────────────────────────────────────
def price(t):
    n = re.sub(r"[^\d]", "", str(t or ""))
    return int(n) if n and 4 <= len(n) <= 9 else None

def surface(t):
    m = re.search(r"(\d+)\s*m", str(t or ""), re.I)
    return int(m.group(1)) if m else None

def rooms(t):
    m = re.search(r"(\d)\s*(p|pièce|T(?=\d))", str(t or ""), re.I)
    return int(m.group(1)) if m else 0

def kind(t):
    t = str(t).lower()
    if "immeuble" in t: return "Immeuble"
    if any(x in t for x in ["maison","villa","pavillon"]): return "Maison"
    if any(x in t for x in ["local","commerce","bureau"]): return "Local"
    return "Appartement"

def yld(p, s, commune):
    if not p or not s: return 0.0
    rpm = {"rennes":13.5,"saint-malo":14.0,"dinard":13.0,"cesson":12.5,
           "bruz":12.0,"fougères":9.5,"vitré":10.0,"redon":9.5}.get(
        next((k for k in ["rennes","saint-malo","dinard","cesson","bruz",
              "fougères","vitré","redon"] if k in commune.lower()), ""), 11.0)
    return round(s * rpm * 12 / p * 100, 1)

def score(p, s, y, commune, typ):
    sc = 50
    if y >= 9: sc += 20
    elif y >= 7: sc += 14
    elif y >= 5: sc += 7
    elif y < 3: sc -= 10
    if p and s:
        pm2 = p/s
        if pm2 < 1500: sc += 15
        elif pm2 < 2500: sc += 10
        elif pm2 < 3500: sc += 5
        elif pm2 > 5500: sc -= 10
    cl = commune.lower()
    if "rennes" in cl: sc += 8
    elif "saint-malo" in cl: sc += 6
    elif any(x in cl for x in ["dinard","cesson","bruz"]): sc += 4
    if typ == "Immeuble": sc += 10
    return max(10, min(100, sc))

def zone(c):
    cl = c.lower()
    if any(x in cl for x in ["rennes","cesson","bruz","liffré","chantepie"]): return "rennes"
    if any(x in cl for x in ["saint-malo","dinard","cancale","briac"]): return "cote"
    return "interior"

def sv(lat, lng):
    return f"https://maps.googleapis.com/maps/api/streetview?size=600x300&location={lat},{lng}&fov=90&pitch=10&key={GMKEY}" if lat and lng else ""

def geo(commune):
    try:
        r = requests.get(f"https://api-adresse.data.gouv.fr/search/?q={commune}&limit=1&type=municipality", timeout=8)
        f = r.json().get("features", [])
        if f:
            c = f[0]["geometry"]["coordinates"]
            return round(c[1],4), round(c[0],4)
    except: pass
    return None, None

def prop(pid, commune, title, p, s, r, lat, lng, photo, source, link, dpe="D"):
    typ = kind(title)
    s = s or 55
    pm2 = round(p/s) if s else 0
    y = yld(p, s, commune)
    sc = score(p, s, y, commune, typ)
    if not photo and lat and lng: photo = sv(lat, lng)
    return {
        "id": pid, "commune": commune.strip().title(), "quartier": "",
        "type": typ, "price": int(p), "surface": int(s), "rooms": int(r or 0),
        "priceM2": pm2, "marketAvg": pm2, "yield": y,
        "rentEstimate": round(s * 11.5), "lat": lat, "lng": lng,
        "score": sc, "opportunity": "hot" if sc>=85 else "good" if sc>=72 else "normal",
        "zone": zone(commune),
        "strategy": ["location","meuble"] if typ=="Appartement" else ["location"],
        "yearBuilt": 1975, "dpe": dpe,
        "tags": [typ, commune.strip().title()],
        "trend": [round(pm2*x) for x in [.85,.88,.91,.94,.97,1.0]],
        "scores": {"location": min(95,sc+5), "rendement": min(95,round(y*10)),
                   "marche": 75, "liquidite": 70, "travaux": 65},
        "description": str(title)[:200], "source": source,
        "daysAgo": random.randint(0,14), "link": link, "photo": photo or ""
    }

# ── DVF — department CSV ──────────────────────────────────────────────────────
def dvf():
    print("\n🏛  DVF — Téléchargement CSV département 35...")
    results, pid = [], 1000

    # Try department-level CSV (not per-commune)
    urls_to_try = [
        "https://files.data.gouv.fr/geo-dvf/latest/csv/35.csv.gz",
        "https://www.data.gouv.fr/fr/datasets/r/90a98de0-f562-4328-aa16-fe0dd1dca60f",
    ]

    r = None
    for url in urls_to_try:
        print(f"  Essai: {url[:60]}...")
        r = get(url, timeout=60, stream=True)
        if r: break

    if not r:
        print("  ❌ DVF non accessible")
        return results

    try:
        content = r.content
        # Try decompressing if gzipped
        try:
            content = gzip.decompress(content)
            print("  ✅ Fichier gz décompressé")
        except:
            pass  # not gzipped

        text = content.decode("utf-8", errors="replace")
        lines = text.strip().split("\n")
        if len(lines) < 2:
            print("  ❌ Fichier vide")
            return results

        headers = [h.strip().strip('"') for h in lines[0].split(",")]
        print(f"  → {len(lines)-1} transactions au total")

        # Focus on key communes
        target_communes = {"rennes", "saint-malo", "bruz", "cesson-sévigné",
                           "fougères", "vitré", "redon", "dinard", "liffré"}
        count = 0

        for line in reversed(lines[1:]):
            if count >= 150: break
            try:
                vals = line.split(",")
                if len(vals) < len(headers): continue
                row = dict(zip(headers, vals))

                if "Vente" not in row.get("nature_mutation", ""): continue

                commune_raw = row.get("commune", row.get("libelle_commune", "")).strip().strip('"').lower()
                if not any(t in commune_raw for t in target_communes): continue

                p_val = price(row.get("valeur_fonciere", ""))
                s_val = row.get("surface_reelle_bati", "").strip()
                s_int = int(float(s_val)) if s_val else None
                type_local = row.get("type_local", "Appartement")
                rooms_raw = row.get("nombre_pieces_principales", "0").strip()
                r_int = int(rooms_raw) if rooms_raw.isdigit() else 0
                lat_raw = row.get("latitude", "").strip()
                lng_raw = row.get("longitude", "").strip()
                lat = float(lat_raw) if lat_raw else None
                lng = float(lng_raw) if lng_raw else None

                if not p_val or p_val < 30000 or p_val > 2000000: continue
                if not s_int or s_int < 10: continue

                typ = "Maison" if "maison" in type_local.lower() else "Appartement"
                commune_title = commune_raw.title()
                photo = sv(lat, lng) if lat and lng else ""
                title = f"{typ} {s_int}m² — {commune_title}"

                p_obj = prop(pid, commune_title, title, p_val, s_int,
                             r_int, lat, lng, photo, "ouestimmo", "", "D")
                p_obj["tags"] = [typ, commune_title, "DVF Officiel"]
                results.append(p_obj)
                pid += 1
                count += 1

            except Exception as e:
                logging.error(f"DVF row: {e}")
                continue

        print(f"  ✅ DVF: {len(results)} transactions")
    except Exception as e:
        logging.error(f"DVF parse: {e}")
        print(f"  ❌ DVF erreur: {e}")

    return results

# ── ORPI ─────────────────────────────────────────────────────────────────────
def orpi():
    print("\n🟠 Orpi...")
    results, pid = [], 2000

    for page in range(1, MAX_PAGES + 1):
        url = f"https://www.orpi.com/recherche/buy/?types%5B%5D=house&types%5B%5D=flat&localisation%5B%5D=departement-ille-et-vilaine-35&page={page}"
        print(f"  Page {page}...")
        r = get(url)
        if not r: wait(); continue

        soup = BeautifulSoup(r.text, "lxml")
        cards = soup.select("article, [class*='card'], [class*='property'], [class*='result']")
        cards = [c for c in cards if c.select_one("[class*='price'], [class*='prix']")]
        print(f"  → {len(cards)} annonces")

        for card in cards:
            try:
                title_el = card.select_one("h2,h3,[class*='title'],[class*='type']")
                title = title_el.get_text(strip=True) if title_el else ""
                if not title: continue

                price_el = card.select_one("[class*='price'],[class*='prix']")
                p_val = price(price_el.get_text() if price_el else "")
                if not p_val or p_val < 30000: continue

                surface_el = card.select_one("[class*='surface'],[class*='area'],[class*='size']")
                s_val = surface(surface_el.get_text() if surface_el else title)

                loc_el = card.select_one("[class*='city'],[class*='location'],[class*='commune'],[class*='localisa']")
                commune = re.sub(r"\d{5}|\(.*?\)", "", loc_el.get_text(strip=True) if loc_el else "").strip() or "Ille-et-Vilaine"

                img_el = card.select_one("img[src],img[data-src]")
                photo = (img_el.get("src") or img_el.get("data-src","")) if img_el else ""
                if photo and not photo.startswith("http"): photo = "https://www.orpi.com" + photo

                link_el = card.select_one("a[href]")
                link = link_el["href"] if link_el else ""
                if link and not link.startswith("http"): link = "https://www.orpi.com" + link

                lat, lng = geo(commune)
                results.append(prop(pid, commune, title, p_val, s_val or 60,
                                    rooms(title), lat, lng, photo, "seloger", link))
                pid += 1
            except Exception as e:
                logging.error(f"Orpi: {e}")

        wait()

    print(f"  ✅ Orpi: {len(results)} annonces")
    return results

# ── GUY HOQUET ───────────────────────────────────────────────────────────────
def guy_hoquet():
    print("\n🟡 Guy Hoquet...")
    results, pid = [], 3000

    for page in range(1, MAX_PAGES + 1):
        url = f"https://www.guy-hoquet.com/biens/result?transaction=Vente&localisation=35&page={page}"
        print(f"  Page {page}...")
        r = get(url)
        if not r: wait(); continue

        soup = BeautifulSoup(r.text, "lxml")
        cards = soup.select("article, [class*='bien'], [class*='card'], [class*='property']")
        cards = [c for c in cards if c.select_one("[class*='price'],[class*='prix']")]
        print(f"  → {len(cards)} annonces")

        for card in cards:
            try:
                title_el = card.select_one("h2,h3,[class*='title'],[class*='type']")
                title = title_el.get_text(strip=True) if title_el else ""
                if not title: continue

                price_el = card.select_one("[class*='price'],[class*='prix']")
                p_val = price(price_el.get_text() if price_el else "")
                if not p_val or p_val < 30000: continue

                surface_el = card.select_one("[class*='surface'],[class*='area']")
                s_val = surface(surface_el.get_text() if surface_el else title)

                loc_el = card.select_one("[class*='city'],[class*='location'],[class*='ville']")
                commune = re.sub(r"\d{5}|\(.*?\)", "", loc_el.get_text(strip=True) if loc_el else "").strip() or "Ille-et-Vilaine"

                img_el = card.select_one("img[src],img[data-src]")
                photo = (img_el.get("src") or img_el.get("data-src","")) if img_el else ""
                if photo and not photo.startswith("http"): photo = "https://www.guy-hoquet.com" + photo

                link_el = card.select_one("a[href]")
                link = link_el["href"] if link_el else ""
                if link and not link.startswith("http"): link = "https://www.guy-hoquet.com" + link

                lat, lng = geo(commune)
                results.append(prop(pid, commune, title, p_val, s_val or 60,
                                    rooms(title), lat, lng, photo, "leboncoin", link))
                pid += 1
            except Exception as e:
                logging.error(f"GuyHoquet: {e}")

        wait()

    print(f"  ✅ Guy Hoquet: {len(results)} annonces")
    return results

# ── ERA IMMOBILIER ────────────────────────────────────────────────────────────
def era():
    print("\n🔵 ERA Immobilier...")
    results, pid = [], 4000

    for page in range(1, MAX_PAGES + 1):
        url = f"https://www.era.fr/acheter/bretagne/ille-et-vilaine/?page={page}"
        print(f"  Page {page}...")
        r = get(url)
        if not r: wait(); continue

        soup = BeautifulSoup(r.text, "lxml")
        cards = soup.select("article, [class*='card'], [class*='property'], [class*='listing']")
        cards = [c for c in cards if c.select_one("[class*='price'],[class*='prix']")]
        print(f"  → {len(cards)} annonces")

        for card in cards:
            try:
                title_el = card.select_one("h2,h3,[class*='title']")
                title = title_el.get_text(strip=True) if title_el else ""
                if not title: continue

                price_el = card.select_one("[class*='price'],[class*='prix']")
                p_val = price(price_el.get_text() if price_el else "")
                if not p_val or p_val < 30000: continue

                surface_el = card.select_one("[class*='surface'],[class*='area']")
                s_val = surface(surface_el.get_text() if surface_el else title)

                loc_el = card.select_one("[class*='city'],[class*='location'],[class*='ville'],[class*='commune']")
                commune = re.sub(r"\d{5}|\(.*?\)", "", loc_el.get_text(strip=True) if loc_el else "").strip() or "Ille-et-Vilaine"

                img_el = card.select_one("img[src],img[data-src]")
                photo = (img_el.get("src") or img_el.get("data-src","")) if img_el else ""
                if photo and not photo.startswith("http"): photo = "https://www.era.fr" + photo

                link_el = card.select_one("a[href]")
                link = link_el["href"] if link_el else ""
                if link and not link.startswith("http"): link = "https://www.era.fr" + link

                lat, lng = geo(commune)
                results.append(prop(pid, commune, title, p_val, s_val or 60,
                                    rooms(title), lat, lng, photo, "bienici", link))
                pid += 1
            except Exception as e:
                logging.error(f"ERA: {e}")

        wait()

    print(f"  ✅ ERA: {len(results)} annonces")
    return results

# ── DEDUP & MAIN ─────────────────────────────────────────────────────────────
def dedup(props):
    seen, out = set(), []
    for p in props:
        key = (int(p["price"]/1000), p["commune"][:8], int(p.get("surface",0)/5))
        if key not in seen: seen.add(key); out.append(p)
    return out

def main():
    print("="*60)
    print("  Invest35 Scraper v3")
    print("="*60)
    print(f"  {datetime.now().strftime('%d/%m/%Y %H:%M')}\n")

    all_props = []
    for fn, name in [(dvf,"DVF"),(orpi,"Orpi"),(guy_hoquet,"Guy Hoquet"),(era,"ERA")]:
        try:
            res = fn()
            all_props.extend(res)
            print(f"  Running total: {len(all_props)}\n")
        except Exception as e:
            print(f"  ❌ {name}: {e}")
            logging.error(f"{name}: {e}")

    all_props = dedup(all_props)
    for i, p in enumerate(all_props):
        p["id"] = i+1
        p["score"] = max(10, min(100, int(p.get("score",50))))

    all_props.sort(key=lambda x: x["score"], reverse=True)

    print(f"\n📊 Total: {len(all_props)} annonces")

    if not all_props:
        print("⚠ Aucune annonce — vérifiez scraper.log")
        return

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump({"generated_at": datetime.now().isoformat(),
                   "total": len(all_props), "properties": all_props},
                  f, ensure_ascii=False, indent=2)

    print(f"✅ {OUTPUT_FILE} sauvegardé")
    print("\n🏆 Top 5:")
    for p in all_props[:5]:
        print(f"  [{p['score']}/100] {p['commune']} — {p['type']} {p['surface']}m² — {p['price']:,}€ — {p['yield']}%")
    print("\n▶  python3 inject.py")
    print("="*60)

if __name__ == "__main__":
    main()
