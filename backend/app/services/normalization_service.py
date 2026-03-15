"""
normalization_service.py
────────────────────────
Normalises brand names, model numbers, machine types, and stock numbers so
that duplicates across languages and crawls map to the same canonical record.

Key additions over the original version
────────────────────────────────────────
• MULTILANG_TYPE_MAP — German / Italian / French / Spanish → English type names
• normalize_machine_type()  now runs the multilingual map first
• extract_stock_number(text) — pull reference / stock numbers from strings
• title_similarity(a, b)    — 0-1 score for fuzzy title matching (no deps)
• build_dedup_key(item)     — deterministic key for cross-language deduplication
"""

import re
import hashlib
from unidecode import unidecode


# ─────────────────────────────────────────────────────────────────────────────
# Brand → machine type hints (used when title doesn't carry type keywords)
# ─────────────────────────────────────────────────────────────────────────────

BRAND_TYPE_HINTS: dict[str, str] = {
    # Injection molding
    "krauss-maffei": "Injection Molding Machine",
    "krauss maffei": "Injection Molding Machine",
    "kraussmaffei":  "Injection Molding Machine",
    "arburg":        "Injection Molding Machine",
    "engel":         "Injection Molding Machine",
    "milacron":      "Injection Molding Machine",
    "husky":         "Injection Molding Machine",
    "battenfeld":    "Injection Molding Machine",
    "demag":         "Injection Molding Machine",
    "netstal":       "Injection Molding Machine",
    "sumitomo":      "Injection Molding Machine",
    # Laser
    "trumpf":        "Laser Cutting Machine",
    "bystronic":     "Laser Cutting Machine",
    "prima power":   "Laser Cutting Machine",
    "mazak optonics":"Laser Cutting Machine",
    "salvagnini":    "Press Brake",
    # Press brake / sheet metal
    "amada":  "Press Brake",
    "safan":  "Press Brake",
    "darley": "Press Brake",
    "haco":   "Press Brake",
    "ermaksan": "Press Brake",
    "durma":  "Press Brake",
    # CNC lathes / machining
    "haas":       "CNC Machining Center",
    "dmg mori":   "CNC Machining Center",
    "mazak":      "CNC Lathe",
    "okuma":      "CNC Lathe",
    "doosan":     "CNC Lathe",
    "hurco":      "CNC Machining Center",
    "makino":     "CNC Machining Center",
    "mori seiki": "CNC Machining Center",
    "fanuc":      "CNC Machining Center",
    "schaublin":  "CNC Lathe",
    "citizen":    "CNC Lathe",
    "star micronics": "CNC Lathe",
    "miyano":  "CNC Lathe",
    "index":   "CNC Lathe",
    "traub":   "CNC Lathe",
    "nakamura":"CNC Lathe",
    # Grinding
    "studer":      "Cylindrical Grinder",
    "schaudt":     "Cylindrical Grinder",
    "jones shipman": "Surface Grinder",
    "blohm":       "Surface Grinder",
    "kellenberger":"Cylindrical Grinder",
    # EDM
    "charmilles":        "Wire EDM",
    "sodick":            "Wire EDM",
    "mitsubishi electric":"Wire EDM",
    "agie":              "Wire EDM",
    "gf machining":      "Wire EDM",
    # Woodworking
    "biesse": "Woodworking Machine",
    "homag":  "Woodworking Machine",
    "scm":    "Woodworking Machine",
    "weinig": "Woodworking Machine",
    # Robotics / welding
    "igm":   "Welding Robot",
    "kuka":  "Industrial Robot",
    "abb":   "Industrial Robot",
    "yaskawa": "Industrial Robot",
    # Filling / packaging
    "kosme":          "Filling Machine",
    "krones":         "Filling Machine",
    "tetra pak":      "Filling Machine",
    "bosch packaging":"Packaging Machine",
    "multivac":       "Packaging Machine",
    "ishida":         "Packaging Machine",
    # Compressors
    "atlas copco":    "Air Compressor",
    "kaeser":         "Air Compressor",
    "ingersoll rand": "Air Compressor",
    # Printing
    "heidelberg": "Printing Machine",
    "roland":     "Printing Machine",
    "komori":     "Printing Machine",
    "manroland":  "Printing Machine",
    # Textile
    "toyota industries": "Textile Machine",
    "picanol": "Textile Machine",
    "sulzer":  "Textile Machine",
}


# ─────────────────────────────────────────────────────────────────────────────
# Brand aliases (many-to-one canonical brand name)
# ─────────────────────────────────────────────────────────────────────────────

BRAND_ALIASES: dict[str, str] = {
    "haas automation": "Haas",
    "haas cnc":        "Haas",
    "dmg mori seiki":  "DMG Mori",
    "dmg mori":        "DMG Mori",
    "dmgmori":         "DMG Mori",
    "mazak":           "Mazak",
    "yamazaki mazak":  "Mazak",
    "fanuc":           "Fanuc",
    "fanuc robotics":  "Fanuc",
    "trumpf":          "Trumpf",
    "trumpf gmbh":     "Trumpf",
    "bystronic":       "Bystronic",
    "amada":           "Amada",
    "okuma":           "Okuma",
    "mori seiki":      "DMG Mori",
    "mitsubishi":      "Mitsubishi",
    "mitsubishi electric": "Mitsubishi",
    "doosan":          "Doosan",
    "doosan infracore": "Doosan",
    "hurco":           "Hurco",
    "makino":          "Makino",
    "toyoda":          "Toyoda",
    "heidenhain":      "Heidenhain",
    "siemens":         "Siemens",
    "arburg":          "Arburg",
    "engel":           "Engel",
    "krauss-maffei":   "KraussMaffei",
    "krauss maffei":   "KraussMaffei",
    "kuka":            "Kuka",
    "sandvik":         "Sandvik",
    "kennametal":      "Kennametal",
    "schaublin":       "Schaublin",
    "citizen":         "Citizen",
    "star micronics":  "Star Micronics",
}


# ─────────────────────────────────────────────────────────────────────────────
# MULTILINGUAL machine type map  (non-English → canonical English)
# ─────────────────────────────────────────────────────────────────────────────

MULTILANG_TYPE_MAP: dict[str, str] = {

    # ── German ────────────────────────────────────────────────────────────────
    "drehmaschine":              "Industrial Lathe",
    "cnc-drehmaschine":         "CNC Lathe",
    "cnc drehmaschine":         "CNC Lathe",
    "drehzentrum":              "CNC Lathe",
    "cnc-drehzentrum":          "CNC Lathe",
    "cnc drehzentrum":          "CNC Lathe",
    "dreh-fraeszentrum":        "CNC Lathe",
    "drehfraeszentrum":         "CNC Lathe",
    "fräsmaschine":             "CNC Milling Machine",
    "fraesmaschine":            "CNC Milling Machine",
    "cnc-fräsmaschine":         "CNC Milling Machine",
    "cnc-fraesmaschine":        "CNC Milling Machine",
    "cnc fräsmaschine":         "CNC Milling Machine",
    "cnc fraesmaschine":        "CNC Milling Machine",
    "fräszentrum":              "CNC Milling Machine",
    "fraeszentrum":             "CNC Milling Machine",
    "bearbeitungszentrum":      "CNC Machining Center",
    "cnc-bearbeitungszentrum":  "CNC Machining Center",
    "vertikales bearbeitungszentrum": "CNC Machining Center",
    "horizontales bearbeitungszentrum": "CNC Machining Center",
    "schleifmaschine":          "Grinder",
    "cnc-schleifmaschine":      "CNC Grinder",
    "rundschleifmaschine":      "Cylindrical Grinder",
    "flachschleifmaschine":     "Surface Grinder",
    "innenrundschleifmaschine": "Internal Grinder",
    "werkzeugschleifmaschine":  "Tool Grinder",
    "bohrmaschine":             "Drilling Machine",
    "cnc-bohrmaschine":         "Drilling Machine",
    "koordinatenbohrmaschine":  "Boring Machine",
    "tischbohrmaschine":        "Drilling Machine",
    "säulebohrmaschine":        "Drilling Machine",
    "stanzmaschine":            "Punch Press",
    "cnc-stanzmaschine":        "Punch Press",
    "stanz-nibbelmaschine":     "Punch Press",
    "nibbelmaschine":           "Punch Press",
    "abkantpresse":             "Press Brake",
    "gesenkbiegepresse":        "Press Brake",
    "hydraulische abkantpresse":"Press Brake",
    "cnc-abkantpresse":         "Press Brake",
    "biegemaschine":            "Bending Machine",
    "rohrbiegemaschine":        "Tube Bending Machine",
    "exzenterpresse":           "Mechanical Press",
    "hydraulikpresse":          "Hydraulic Press",
    "presse":                   "Press",
    "schermaschine":            "Shearing Machine",
    "tafelschere":              "Shearing Machine",
    "laserschneidmaschine":     "Laser Cutting Machine",
    "laserschneidanlage":       "Laser Cutting Machine",
    "laserschneider":           "Laser Cutting Machine",
    "laser-schneidanlage":      "Laser Cutting Machine",
    "faserlaser":               "Laser Cutting Machine",
    "co2-laser":                "Laser Cutting Machine",
    "plasmaschneidmaschine":    "Plasma Cutting Machine",
    "plasmaschneidanlage":      "Plasma Cutting Machine",
    "wasserstrahlschneidmaschine": "Waterjet Cutting Machine",
    "wasserstrahl":             "Waterjet Cutting Machine",
    "spritzgiessmaschine":      "Injection Molding Machine",
    "spritzgussmaschine":       "Injection Molding Machine",
    "spritzguss":               "Injection Molding Machine",
    "kunststoffspritzmaschine": "Injection Molding Machine",
    "blasformmaschine":         "Blow Molding Machine",
    "extruder":                 "Extruder",
    "säge":                     "Saw",
    "saege":                    "Saw",
    "bandsäge":                 "Band Saw",
    "bandsaege":                "Band Saw",
    "kreissäge":                "Circular Saw",
    "kreissaege":               "Circular Saw",
    "kappsäge":                 "Miter Saw",
    "schweissmaschine":         "Welding Machine",
    "schweißmaschine":          "Welding Machine",
    "roboterschweissen":        "Welding Robot",
    "roboter":                  "Industrial Robot",
    "industrieroboter":         "Industrial Robot",
    "messmaschine":             "CMM",
    "koordinatenmessmaschine":  "CMM",
    "graviermaschine":          "Engraving Machine",
    "transfermaschine":         "Transfer Machine",
    "transfer-maschine":        "Transfer Machine",
    "zahnradfraesmaschine":     "Gear Milling Machine",
    "zahnradschleifmaschine":   "Gear Grinding Machine",
    "honmaschine":              "Honing Machine",
    "läppmaschine":             "Lapping Machine",
    "entgratmaschine":          "Deburring Machine",
    "waschmaschine industriell":"Industrial Washing Machine",
    "werkzeugmaschine":         "Machine Tool",
    "werkzeugmaschinen":        "Machine Tool",
    "zerspanungsmaschine":      "Machining Equipment",

    # ── Italian ───────────────────────────────────────────────────────────────
    "tornio":                   "Industrial Lathe",
    "tornio cnc":               "CNC Lathe",
    "tornio a cnc":             "CNC Lathe",
    "centro di tornitura":      "CNC Lathe",
    "centro tornitura":         "CNC Lathe",
    "tornio automatico":        "CNC Lathe",
    "tornio parallelo":         "Industrial Lathe",
    "fresatrice":               "CNC Milling Machine",
    "fresatrice cnc":           "CNC Milling Machine",
    "centro di lavoro":         "CNC Machining Center",
    "centro di lavoro cnc":     "CNC Machining Center",
    "centro di lavoro verticale": "CNC Machining Center",
    "centro di lavoro orizzontale": "CNC Machining Center",
    "rettificatrice":           "Grinder",
    "rettificatrice cilindrica":"Cylindrical Grinder",
    "rettificatrice piana":     "Surface Grinder",
    "rettificatrice cnc":       "CNC Grinder",
    "trapano":                  "Drilling Machine",
    "trapano a colonna":        "Drilling Machine",
    "alesatrice":               "Boring Machine",
    "punzonatrice":             "Punch Press",
    "punzonatrice cnc":         "Punch Press",
    "pressa piegatrice":        "Press Brake",
    "piegatrice":               "Press Brake",
    "piegatrice cnc":           "Press Brake",
    "pressa":                   "Press",
    "pressa idraulica":         "Hydraulic Press",
    "cesoia":                   "Shearing Machine",
    "cesoia guillottina":       "Shearing Machine",
    "laser di taglio":          "Laser Cutting Machine",
    "macchina laser":           "Laser Cutting Machine",
    "taglio laser":             "Laser Cutting Machine",
    "plasma di taglio":         "Plasma Cutting Machine",
    "waterjet":                 "Waterjet Cutting Machine",
    "pressa ad iniezione":      "Injection Molding Machine",
    "pressa a iniezione":       "Injection Molding Machine",
    "sega":                     "Saw",
    "sega a nastro":            "Band Saw",
    "sega circolare":           "Circular Saw",
    "saldatrice":               "Welding Machine",
    "robot":                    "Industrial Robot",
    "robot industriale":        "Industrial Robot",
    "macchina per misurare":    "CMM",
    "macchina di misura":       "CMM",

    # ── French ────────────────────────────────────────────────────────────────
    "tour":                     "Industrial Lathe",
    "tour cnc":                 "CNC Lathe",
    "tour à commande numérique":"CNC Lathe",
    "centre de tournage":       "CNC Lathe",
    "fraiseuse":                "CNC Milling Machine",
    "fraiseuse cnc":            "CNC Milling Machine",
    "centre d'usinage":         "CNC Machining Center",
    "centre dusinage":          "CNC Machining Center",
    "centre d usinage":         "CNC Machining Center",
    "centre de fraisage":       "CNC Milling Machine",
    "rectifieuse":              "Grinder",
    "rectifieuse cylindrique":  "Cylindrical Grinder",
    "rectifieuse plane":        "Surface Grinder",
    "perceuse":                 "Drilling Machine",
    "perceuse à colonne":       "Drilling Machine",
    "aléseuse":                 "Boring Machine",
    "poinçonneuse":             "Punch Press",
    "presse plieuse":           "Press Brake",
    "plieuse":                  "Press Brake",
    "plieuse cnc":              "Press Brake",
    "presse":                   "Press",
    "presse hydraulique":       "Hydraulic Press",
    "cisaille":                 "Shearing Machine",
    "guillotine":               "Shearing Machine",
    "machine de découpe laser": "Laser Cutting Machine",
    "découpe laser":            "Laser Cutting Machine",
    "laser de découpe":         "Laser Cutting Machine",
    "découpe plasma":           "Plasma Cutting Machine",
    "jet d'eau":                "Waterjet Cutting Machine",
    "presse à injection":       "Injection Molding Machine",
    "scie":                     "Saw",
    "scie à ruban":             "Band Saw",
    "scie circulaire":          "Circular Saw",
    "soudeuse":                 "Welding Machine",
    "robot":                    "Industrial Robot",
    "machine à mesurer":        "CMM",

    # ── Spanish ───────────────────────────────────────────────────────────────
    "torno":                    "Industrial Lathe",
    "torno cnc":                "CNC Lathe",
    "torno de control numérico":"CNC Lathe",
    "centro de torneado":       "CNC Lathe",
    "fresadora":                "CNC Milling Machine",
    "fresadora cnc":            "CNC Milling Machine",
    "centro de mecanizado":     "CNC Machining Center",
    "centro de maquinado":      "CNC Machining Center",
    "rectificadora":            "Grinder",
    "rectificadora cilíndrica": "Cylindrical Grinder",
    "rectificadora plana":      "Surface Grinder",
    "taladro":                  "Drilling Machine",
    "taladro de columna":       "Drilling Machine",
    "mandrinadora":             "Boring Machine",
    "punzonadora":              "Punch Press",
    "plegadora":                "Press Brake",
    "prensa plegadora":         "Press Brake",
    "plegadora cnc":            "Press Brake",
    "prensa":                   "Press",
    "prensa hidráulica":        "Hydraulic Press",
    "cizalla":                  "Shearing Machine",
    "guillotina":               "Shearing Machine",
    "cortadora láser":          "Laser Cutting Machine",
    "máquina láser":            "Laser Cutting Machine",
    "corte láser":              "Laser Cutting Machine",
    "corte plasma":             "Plasma Cutting Machine",
    "corte por agua":           "Waterjet Cutting Machine",
    "inyectora":                "Injection Molding Machine",
    "prensa de inyección":      "Injection Molding Machine",
    "sierra":                   "Saw",
    "sierra de cinta":          "Band Saw",
    "sierra circular":          "Circular Saw",
    "soldadora":                "Welding Machine",
    "robot":                    "Industrial Robot",
    "robot industrial":         "Industrial Robot",
    "máquina de medición":      "CMM",

    # ── Dutch ─────────────────────────────────────────────────────────────────
    "draaibank":                "Industrial Lathe",
    "cnc draaibank":            "CNC Lathe",
    "freesmachine":             "CNC Milling Machine",
    "bewerkingscentrum":        "CNC Machining Center",
    "slijpmachine":             "Grinder",
    "boorstaander":             "Drilling Machine",
    "kantpers":                 "Press Brake",
    "lasersnijmachine":         "Laser Cutting Machine",
    "spuitgietmachine":         "Injection Molding Machine",
    "zaagmachine":              "Saw",
    "bandschijfmachine":        "Band Saw",
    "lasmachine":               "Welding Machine",

    # ── English (catch common spelling variants not in TYPE_SYNONYMS) ─────────
    "turning machine":          "CNC Lathe",
    "turning lathe":            "Industrial Lathe",
    "mill":                     "CNC Milling Machine",
    "milling":                  "CNC Milling Machine",
    "machining centre":         "CNC Machining Center",   # British spelling
    "vertical mill":            "CNC Milling Machine",
    "vertical milling machine": "CNC Milling Machine",
    "horizontal mill":          "CNC Milling Machine",
    "gear hobbing machine":     "Gear Hobbing Machine",
    "gear hob":                 "Gear Hobbing Machine",
    "thread grinder":           "Thread Grinding Machine",
    "profile grinder":          "Profile Grinder",
    "surface grinding machine": "Surface Grinder",
    "hydraulic bending machine":"Press Brake",
    "folding machine":          "Press Brake",
    "nibbling machine":         "Punch Press",
    "turret punch press":       "Punch Press",
    "flame cutting machine":    "Plasma Cutting Machine",
    "abrasive waterjet":        "Waterjet Cutting Machine",
    "wire cut edm":             "Wire EDM",
    "wire cutting edm":         "Wire EDM",
    "spark erosion":            "EDM Machine",
    "die sinking edm":          "Sinker EDM",
    "injection press":          "Injection Molding Machine",
    "transfer moulding press":  "Injection Molding Machine",
}


# English-only synonyms (kept for backwards compat + partial match fallback)
TYPE_SYNONYMS: dict[str, str] = {
    "cnc machining center":       "CNC Machining Center",
    "machining center":           "CNC Machining Center",
    "milling center":             "CNC Machining Center",
    "cnc milling machine":        "CNC Milling Machine",
    "milling machine":            "CNC Milling Machine",
    "vertical machining center":  "CNC Machining Center",
    "vmc":                        "CNC Machining Center",
    "horizontal machining center":"CNC Machining Center",
    "hmc":                        "CNC Machining Center",
    "cnc lathe":                  "CNC Lathe",
    "turning center":             "CNC Lathe",
    "cnc turning center":         "CNC Lathe",
    "lathe":                      "Industrial Lathe",
    "industrial lathe":           "Industrial Lathe",
    "engine lathe":               "Industrial Lathe",
    "laser cutter":               "Laser Cutting Machine",
    "laser cutting machine":      "Laser Cutting Machine",
    "laser cutting center":       "Laser Cutting Machine",
    "fiber laser":                "Laser Cutting Machine",
    "co2 laser":                  "Laser Cutting Machine",
    "laser":                      "Laser Cutting Machine",
    "press brake":                "Press Brake",
    "pressbrake":                 "Press Brake",
    "bending machine":            "Press Brake",
    "cnc bending machine":        "Press Brake",
    "hydraulic press brake":      "Press Brake",
    "injection molding machine":  "Injection Molding Machine",
    "injection moulding machine": "Injection Molding Machine",
    "injection molding":          "Injection Molding Machine",
    "plastic injection machine":  "Injection Molding Machine",
    "surface grinder":            "Surface Grinder",
    "cylindrical grinder":        "Cylindrical Grinder",
    "cnc grinder":                "CNC Grinder",
    "grinder":                    "Grinder",
    "grinding machine":           "Grinder",
    "edm":                        "EDM Machine",
    "wire edm":                   "Wire EDM",
    "sinker edm":                 "Sinker EDM",
    "punching machine":           "Punch Press",
    "punch press":                "Punch Press",
    "shearing machine":           "Shearing Machine",
    "waterjet":                   "Waterjet Cutting Machine",
    "water jet cutter":           "Waterjet Cutting Machine",
    "plasma cutter":              "Plasma Cutting Machine",
    "drill press":                "Drill Press",
    "boring machine":             "Boring Machine",
    "band saw":                   "Band Saw",
    "bandsaw":                    "Band Saw",
    "circular saw":               "Circular Saw",
    "welding machine":            "Welding Machine",
    "industrial robot":           "Industrial Robot",
    "cmm":                        "CMM",
    "coordinate measuring machine": "CMM",
}


# ─────────────────────────────────────────────────────────────────────────────
# Stock number patterns
# ─────────────────────────────────────────────────────────────────────────────

# Matches patterns like: ST-1234, REF-ABC123, #12345, SN:A1234, Stock: AB-123
_STOCK_RE = re.compile(
    r"(?:"
    r"(?:stock|ref|sku|sn|serial|no\.?|nr\.?|id|item)\s*[:#\-]?\s*([A-Z0-9][A-Z0-9\-_]{2,20})"
    r"|#\s*([A-Z0-9]{3,20})"
    r"|(?<!\w)([A-Z]{1,3}[\-_]?[0-9]{3,8}(?:[\-_][A-Z0-9]+)?)"
    r")",
    re.IGNORECASE,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _clean(text: str) -> str:
    """Lowercase, ASCII-fold (ä→a, ü→u, é→e…), collapse whitespace."""
    return re.sub(r"\s+", " ", unidecode(text).lower().strip())


# ─────────────────────────────────────────────────────────────────────────────
# Public normalization functions
# ─────────────────────────────────────────────────────────────────────────────

def normalize_brand(brand: str | None) -> str | None:
    if not brand:
        return None
    key = _clean(brand)
    return BRAND_ALIASES.get(key, brand.strip().title())


def normalize_model(model: str | None) -> str | None:
    if not model:
        return None
    m = re.sub(r"\s+", " ", model.strip())
    m = re.sub(r"[\u2013\u2014]", "-", m)   # em/en dash → hyphen
    m = re.sub(r"\s*-\s*", "-", m)
    return m.upper()


def normalize_machine_type(machine_type: str | None) -> str | None:
    """
    Normalize a machine type string to a canonical English name.

    Order of lookup:
      1. MULTILANG_TYPE_MAP  — handles German / Italian / French / Spanish
      2. TYPE_SYNONYMS       — English synonyms and abbreviations
      3. Partial match in TYPE_SYNONYMS
      4. Title-cased original (fallback)
    """
    if not machine_type:
        return None

    key = _clean(machine_type)

    # 1. Multilingual exact match
    if key in MULTILANG_TYPE_MAP:
        return MULTILANG_TYPE_MAP[key]

    # 2. English synonyms exact match
    if key in TYPE_SYNONYMS:
        return TYPE_SYNONYMS[key]

    # 3. Multilingual partial match (longer entries first to avoid false hits)
    for term in sorted(MULTILANG_TYPE_MAP.keys(), key=len, reverse=True):
        if term in key:
            return MULTILANG_TYPE_MAP[term]

    # 4. English partial match
    for synonym, canonical in sorted(TYPE_SYNONYMS.items(), key=lambda x: len(x[0]), reverse=True):
        if synonym in key:
            return canonical

    return machine_type.strip().title()


def infer_type_from_brand(brand: str | None, title: str | None = None) -> str | None:
    """
    Infer machine type from brand name using BRAND_TYPE_HINTS.
    Falls back to TYPE_SYNONYMS keyword scan on the title.
    """
    if brand:
        key = _clean(brand)
        if key in BRAND_TYPE_HINTS:
            return BRAND_TYPE_HINTS[key]
        for hint_brand, machine_type in BRAND_TYPE_HINTS.items():
            if hint_brand in key or key in hint_brand:
                return machine_type

    if title:
        result = normalize_machine_type(title)
        if result and result != title.strip().title():
            return result

    return None


def extract_stock_number(text: str | None) -> str | None:
    """
    Extract a stock / reference number from free-form text.

    Handles patterns like:
      "Stock No: AB-1234"    → "AB-1234"
      "Ref #12345"           → "12345"
      "SKU: MZ-QT28-2019"   → "MZ-QT28-2019"
      "SN: A123456"          → "A123456"
      Title contains "MAZAK QT28 (ST-1024)" → "ST-1024"
    """
    if not text:
        return None
    for m in _STOCK_RE.finditer(text):
        val = m.group(1) or m.group(2) or m.group(3)
        if val:
            val = val.strip().upper()
            # Reject values that are purely numeric and very short (unlikely to be a real stock no)
            if re.match(r"^\d{1,3}$", val):
                continue
            return val
    return None


def title_similarity(a: str | None, b: str | None) -> float:
    """
    Simple token-based similarity score between two machine title strings.

    Returns 0.0 (completely different) to 1.0 (identical after cleaning).

    Algorithm: Jaccard similarity on word sets (no external libraries needed).
    Adequate for detecting near-duplicate machine titles across languages
    when brand+model are already matched.
    """
    if not a or not b:
        return 0.0

    def _tokens(s: str) -> set[str]:
        s = unidecode(s).lower()
        # Remove short noise words that differ by language
        s = re.sub(r"\b(the|a|an|used|gebraucht|usato|occasion|cnc|for|sale)\b", " ", s)
        return set(t for t in re.split(r"[\W_]+", s) if len(t) >= 3)

    ta, tb = _tokens(a), _tokens(b)
    if not ta or not tb:
        return 0.0

    intersection = ta & tb
    union        = ta | tb
    return len(intersection) / len(union)


def build_content_hash(brand: str | None, model: str | None, url: str) -> str:
    """
    Stable SHA-256 hash used as the primary duplicate detection key.

    Uses brand + model (not URL) so the same machine at different URLs
    (e.g. English vs German version) hashes to the same value.
    """
    parts = [
        (brand or "").upper().strip(),
        (model or "").upper().strip(),
        url.strip().lower(),
    ]
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()


def build_dedup_key(brand: str | None, model: str | None,
                    stock_number: str | None = None) -> str:
    """
    Cross-language deduplication key based on brand + model + optional stock number.

    This key is URL-independent — the same machine found at /de/maschine/123
    and /en/machine/456 will produce the same dedup_key if brand+model+stock match.

    Used by DatabasePipeline to detect cross-language duplicates before inserting.
    """
    parts = [
        normalize_brand(brand) or "",
        normalize_model(model) or "",
        (stock_number or "").upper().strip(),
    ]
    raw = "|".join(p.upper().strip() for p in parts)
    return hashlib.sha256(raw.encode()).hexdigest()
