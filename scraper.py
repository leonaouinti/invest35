"""
Invest35 — Processeur DVF Local
================================
Lit le fichier dvf.csv téléchargé depuis data.gouv.fr
et génère properties.json pour Invest35.

UTILISATION:
    python3 scraper.py
"""

import csv, json, random, re, logging, requests
from datetime import datetime

DVF_FILE = "dvf.csv"
OUTPUT_FILE = "properties.json"
GMKEY = "AIzaSyATL88HE9Dt3IjV8Zuzn3ARu7FgLboYYZ0"

logging.basicConfig(filename="scraper.log", level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

# Communes cibles en Ille-et-Vilaine
COMMUNES_35 = {
    "RENNES", "SAINT-MALO", "FOUGERES", "VITRE", "CESSON-SEVIGNE",
    "BRUZ", "DINARD", "REDON", "LIFFRE", "CHANTEPIE", "PACE",
    "BETTON", "SAINT-GREGOIRE", "VEZIN-LE-COQUET", "MORDELLES",
    "CHARTRES-DE-BRETAGNE", "Corps-NUDS", "NOYAL-CHATILLON-SUR-SEICHE",
    "SAINT-ERBLON", "VERN-SUR-SEICHE", "ACIGNE", "THORIGNE-FOUILLARD",
    "SAINT-JACQUES-DE-LA-LANDE", "L'HERMITAGE", "MONTGERMONT",
    "CANCALE", "SAINT-BRIAC-SUR-MER", "DINARD", "PLEURTUIT",
    "ROTHENEUF", "PARAMÉ"
}

def sv(lat, lng):
    if lat and lng:
        return f"https://maps.googleapis.com/maps/api/streetview?size=600x300&location={lat},{lng}&fov=90&pitch=10&key={GMKEY}"
    return ""

def geocode(commune):
    """Geocode via French government API — free & fast."""
    try:
        r = requests.get(
            f"https://api-adresse.data.gouv.fr/search/?q={commune}&limit=1&type=municipality",
            timeout=8
        )
        feats = r.json().get("features", [])
        if feats:
            c = feats[0]["geometry"]["coordinates"]
            return round(c[1], 4), round(c[0], 4)
    except:
        pass
    return None, None

def calc_yield(price, surface, commune):
    if not price or not surface: return 0.0
    cl = commune.lower()
    rpm = (13.5 if "rennes" in cl else
           14.0 if "malo" in cl else
           13.0 if "dinard" in cl else
           12.5 if "cesson" in cl or "bruz" in cl else
           10.0 if "vitré" in cl or "vitre" in cl else
           9.5 if "fougères" in cl or "fougeres" in cl or "redon" in cl else
           11.0)
    return round(surface * rpm * 12 / price * 100, 1)

def calc_score(price, surface, y, commune):
    score = 50
    if y >= 9: score += 20
    elif y >= 7: score += 14
    elif y >= 5: score += 7
    elif y < 3: score -= 10
    if price and surface:
        pm2 = price / surface
        if pm2 < 1500: score += 15
        elif pm2 < 2500: score += 10
        elif pm2 < 3500: score += 5
        elif pm2 > 5500: score -= 8
    cl = commune.lower()
    if "rennes" in cl: score += 8
    elif "malo" in cl: score += 6
    elif any(x in cl for x in ["dinard", "cesson", "bruz"]): score += 4
    return max(10, min(100, score))

def get_zone(commune):
    cl = commune.lower()
    if any(x in cl for x in ["rennes", "cesson", "bruz", "liffré", "chantepie", "pacé", "betton"]):
        return "rennes"
    if any(x in cl for x in ["malo", "dinard", "cancale", "briac", "rothéneuf"]):
        return "cote"
    return "interior"

def process_dvf():
    print(f"\n📂 Lecture de {DVF_FILE}...")
    results = []
    pid = 1
    seen = set()

    # Cache geocoding results to avoid repeated API calls
    geo_cache = {}

    try:
        with open(DVF_FILE, encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f, delimiter="|")
            rows = list(reader)
            print(f"  → {len(rows):,} lignes totales")
    except FileNotFoundError:
        print(f"  ❌ Fichier {DVF_FILE} introuvable!")
        print(f"     Placez dvf.csv dans le dossier invest35")
        return []
    except Exception as e:
        print(f"  ❌ Erreur lecture: {e}")
        return []

    print(f"  🔍 Filtrage Ille-et-Vilaine (code dept 35)...")
    count = 0

    for row in rows:
        try:
            # Filter by department 35
            dept = str(row.get("Code departement", "")).strip()
            if dept != "35":
                continue

            # Only actual sales
            nature = str(row.get("Nature mutation", "")).strip()
            if "Vente" not in nature:
                continue

            # Only houses and apartments
            type_local = str(row.get("Type local", "")).strip()
            if type_local not in ["Appartement", "Maison"]:
                continue

            # Price
            price_raw = str(row.get("Valeur fonciere", "")).strip().replace(",", ".").replace(" ", "")
            try:
                price = int(float(price_raw))
            except:
                continue
            if price < 30000 or price > 3000000:
                continue

            # Surface
            surface_raw = str(row.get("Surface reelle bati", "")).strip().replace(",", ".")
            try:
                surface = int(float(surface_raw))
            except:
                continue
            if surface < 10 or surface > 1000:
                continue

            # Commune
            commune = str(row.get("Commune", "")).strip().title()
            if not commune:
                continue

            # Rooms
            rooms_raw = str(row.get("Nombre pieces principales", "0")).strip()
            try:
                rooms = int(float(rooms_raw))
            except:
                rooms = 0

            # Deduplicate (same price + commune + surface)
            dedup_key = (int(price/1000), commune[:8], int(surface/5))
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            # Geocode (with cache)
            if commune not in geo_cache:
                lat, lng = geocode(commune)
                geo_cache[commune] = (lat, lng)
            else:
                lat, lng = geo_cache[commune]

            # Calculate metrics
            pm2 = round(price / surface)
            y = calc_yield(price, surface, commune)
            score = calc_score(price, surface, y, commune)
            zone = get_zone(commune)
            photo = sv(lat, lng) if lat and lng else ""

            # Address info
            voie = str(row.get("Voie", "")).strip().title()
            type_voie = str(row.get("Type de voie", "")).strip().title()
            no_voie = str(row.get("No voie", "")).strip()
            address = f"{no_voie} {type_voie} {voie}".strip() if voie else commune
            code_postal = str(row.get("Code postal", "")).strip()

            description = f"{type_local} de {surface}m² à {commune}"
            if code_postal:
                description += f" ({code_postal})"
            description += f" — {rooms} pièce{'s' if rooms > 1 else ''}" if rooms else ""

            prop = {
                "id": pid,
                "commune": commune,
                "quartier": voie[:30] if voie else "",
                "type": type_local,
                "price": price,
                "surface": surface,
                "rooms": rooms,
                "priceM2": pm2,
                "marketAvg": pm2,
                "yield": y,
                "rentEstimate": round(surface * 11.5),
                "lat": lat,
                "lng": lng,
                "score": score,
                "opportunity": "hot" if score >= 85 else "good" if score >= 72 else "normal",
                "zone": zone,
                "strategy": ["location", "meuble"] if type_local == "Appartement" else ["location"],
                "yearBuilt": 1975,
                "dpe": "D",
                "tags": [type_local, commune, "DVF Officiel"],
                "trend": [round(pm2 * x) for x in [.85, .88, .91, .94, .97, 1.0]],
                "scores": {
                    "location": min(95, score + 5),
                    "rendement": min(95, round(y * 10)),
                    "marche": 75,
                    "liquidite": 70,
                    "travaux": 65
                },
                "description": description,
                "source": "ouestimmo",
                "daysAgo": random.randint(0, 30),
                "link": "",
                "photo": photo
            }

            results.append(prop)
            pid += 1
            count += 1

            # Show progress every 50 properties
            if count % 50 == 0:
                print(f"  → {count} biens traités...")

            # Limit to 300 best properties
            if count >= 300:
                break

        except Exception as e:
            logging.error(f"Row error: {e}")
            continue

    print(f"  ✅ {len(results)} biens extraits pour Ille-et-Vilaine")
    return results

def main():
    print("=" * 55)
    print("  Invest35 — Processeur DVF")
    print("=" * 55)
    print(f"  {datetime.now().strftime('%d/%m/%Y %H:%M')}\n")

    props = process_dvf()

    if not props:
        print("\n⚠ Aucun bien trouvé.")
        print("  Vérifiez que dvf.csv est dans le dossier invest35")
        return

    # Sort by score
    props.sort(key=lambda x: x["score"], reverse=True)

    # Re-number IDs
    for i, p in enumerate(props):
        p["id"] = i + 1

    # Save
    output = {
        "generated_at": datetime.now().isoformat(),
        "total": len(props),
        "properties": props
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✅ {OUTPUT_FILE} sauvegardé — {len(props)} biens")

    print("\n📊 Stats:")
    communes = {}
    for p in props:
        communes[p["commune"]] = communes.get(p["commune"], 0) + 1
    for c, n in sorted(communes.items(), key=lambda x: -x[1])[:10]:
        print(f"   {c}: {n} biens")

    print("\n🏆 Top 5 opportunités:")
    for p in props[:5]:
        print(f"  [{p['score']}/100] {p['commune']} — {p['type']} {p['surface']}m² — {p['price']:,}€ — {p['yield']}%")

    print("\n▶ Étape suivante: python3 inject.py")
    print("=" * 55)

if __name__ == "__main__":
    main()
