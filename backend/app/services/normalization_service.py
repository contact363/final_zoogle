"""
Normalizes brand names, model numbers, and machine types so duplicates map
to the same canonical entry regardless of how they were scraped.
"""
import re
from unidecode import unidecode

# ── Brand aliases ────────────────────────────────────────────────────────────
BRAND_ALIASES: dict[str, str] = {
    "haas automation": "Haas",
    "haas cnc": "Haas",
    "dmg mori seiki": "DMG Mori",
    "dmg mori": "DMG Mori",
    "dmgmori": "DMG Mori",
    "mazak": "Mazak",
    "yamazaki mazak": "Mazak",
    "fanuc": "Fanuc",
    "fanuc robotics": "Fanuc",
    "trumpf": "Trumpf",
    "trumpf gmbh": "Trumpf",
    "bystronic": "Bystronic",
    "amada": "Amada",
    "okuma": "Okuma",
    "mori seiki": "DMG Mori",
    "mitsubishi": "Mitsubishi",
    "mitsubishi electric": "Mitsubishi",
    "mazak": "Mazak",
    "doosan": "Doosan",
    "doosan infracore": "Doosan",
    "hurco": "Hurco",
    "makino": "Makino",
    "toyoda": "Toyoda",
    "heidenhain": "Heidenhain",
    "siemens": "Siemens",
    "arburg": "Arburg",
    "engel": "Engel",
    "krauss-maffei": "KraussMaffei",
    "krauss maffei": "KraussMaffei",
    "kuka": "Kuka",
    "sandvik": "Sandvik",
    "kennametal": "Kennametal",
    "schaublin": "Schaublin",
    "citizen": "Citizen",
    "star micronics": "Star Micronics",
}

# ── Machine type synonyms ────────────────────────────────────────────────────
TYPE_SYNONYMS: dict[str, str] = {
    # CNC
    "cnc machining center": "CNC Machining Center",
    "machining center": "CNC Machining Center",
    "milling center": "CNC Machining Center",
    "cnc milling machine": "CNC Milling Machine",
    "milling machine": "CNC Milling Machine",
    "vertical machining center": "CNC Machining Center",
    "vmc": "CNC Machining Center",
    "horizontal machining center": "CNC Machining Center",
    "hmc": "CNC Machining Center",
    "cnc lathe": "CNC Lathe",
    "turning center": "CNC Lathe",
    "cnc turning center": "CNC Lathe",
    "lathe": "Industrial Lathe",
    "industrial lathe": "Industrial Lathe",
    "engine lathe": "Industrial Lathe",
    # Laser
    "laser cutter": "Laser Cutting Machine",
    "laser cutting machine": "Laser Cutting Machine",
    "laser cutting center": "Laser Cutting Machine",
    "fiber laser": "Laser Cutting Machine",
    "co2 laser": "Laser Cutting Machine",
    "laser": "Laser Cutting Machine",
    # Press
    "press brake": "Press Brake",
    "pressbrake": "Press Brake",
    "bending machine": "Press Brake",
    "cnc bending machine": "Press Brake",
    "hydraulic press brake": "Press Brake",
    # Injection
    "injection molding machine": "Injection Molding Machine",
    "injection moulding machine": "Injection Molding Machine",
    "injection molding": "Injection Molding Machine",
    "plastic injection machine": "Injection Molding Machine",
    # Grinding
    "surface grinder": "Surface Grinder",
    "cylindrical grinder": "Cylindrical Grinder",
    "cnc grinder": "CNC Grinder",
    "grinder": "Grinder",
    "grinding machine": "Grinder",
    # EDM
    "edm": "EDM Machine",
    "wire edm": "Wire EDM",
    "sinker edm": "Sinker EDM",
    # Other
    "punching machine": "Punch Press",
    "punch press": "Punch Press",
    "shearing machine": "Shearing Machine",
    "waterjet": "Waterjet Cutting Machine",
    "water jet cutter": "Waterjet Cutting Machine",
    "plasma cutter": "Plasma Cutting Machine",
    "drill press": "Drill Press",
    "boring machine": "Boring Machine",
}


def _clean(text: str) -> str:
    """Lowercase, ascii-fold, collapse whitespace."""
    return re.sub(r"\s+", " ", unidecode(text).lower().strip())


def normalize_brand(brand: str | None) -> str | None:
    if not brand:
        return None
    key = _clean(brand)
    return BRAND_ALIASES.get(key, brand.strip().title())


def normalize_model(model: str | None) -> str | None:
    if not model:
        return None
    # Remove extra spaces, normalize hyphens
    m = re.sub(r"\s+", " ", model.strip())
    m = re.sub(r"[\u2013\u2014]", "-", m)   # em/en dash → hyphen
    m = re.sub(r"\s*-\s*", "-", m)           # spaces around hyphen
    return m.upper()


def normalize_machine_type(machine_type: str | None) -> str | None:
    if not machine_type:
        return None
    key = _clean(machine_type)
    # exact match first
    if key in TYPE_SYNONYMS:
        return TYPE_SYNONYMS[key]
    # partial match
    for synonym, canonical in TYPE_SYNONYMS.items():
        if synonym in key:
            return canonical
    return machine_type.strip().title()


def build_content_hash(brand: str | None, model: str | None, url: str) -> str:
    """Stable hash used to detect duplicate machine listings."""
    import hashlib
    parts = [
        (brand or "").upper().strip(),
        (model or "").upper().strip(),
        url.strip().lower(),
    ]
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()
