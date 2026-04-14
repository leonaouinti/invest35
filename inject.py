"""
inject.py — Injecte les données scrapées dans index.html
=========================================================
UTILISATION:
    python inject.py

Lit properties.json et met à jour le tableau PROPERTIES dans index.html
"""

import json
import re
import sys
from datetime import datetime

PROPERTIES_FILE = "properties.json"
HTML_FILE = "index.html"

def inject():
    # Load scraped data
    try:
        with open(PROPERTIES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        properties = data.get("properties", data) if isinstance(data, dict) else data
        print(f"✅ {len(properties)} annonces chargées depuis {PROPERTIES_FILE}")
    except FileNotFoundError:
        print(f"❌ {PROPERTIES_FILE} introuvable. Lancez d'abord: python scraper.py")
        sys.exit(1)

    # Load HTML
    try:
        with open(HTML_FILE, "r", encoding="utf-8") as f:
            html = f.read()
    except FileNotFoundError:
        print(f"❌ {HTML_FILE} introuvable.")
        sys.exit(1)

    # Build JS array
    js_array = json.dumps(properties, ensure_ascii=False, indent=2)

    # Replace PROPERTIES array in HTML
    # Matches from "const PROPERTIES" to "].map(p=>" or "];"
    pattern = r'(const PROPERTIES\s*=\s*\[).*?(\](?:\.map\(p=>\(\{\.\.\.p,\s*photo:sv\(p\.lat,p\.lng\)\}\))?;)'
    replacement = f'const PROPERTIES = {js_array};'

    new_html, count = re.subn(pattern, replacement, html, flags=re.DOTALL)

    if count == 0:
        print("⚠ Pattern PROPERTIES non trouvé dans index.html")
        print("  Assurez-vous que index.html contient 'const PROPERTIES = ['")
        sys.exit(1)

    # Update header count
    new_html = re.sub(
        r'(<div class="hstat-val" id="h-count">)\d+(</div>)',
        f'\\g<1>{len(properties)}\\g<2>',
        new_html
    )

    # Write updated HTML
    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(new_html)

    print(f"✅ index.html mis à jour avec {len(properties)} annonces")
    print(f"   Généré le: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"\n📋 Prochaine étape: git add index.html && git commit -m 'update listings' && git push")

if __name__ == "__main__":
    inject()
