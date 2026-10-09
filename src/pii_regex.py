import re

# ---------------------------------------------------------------------------
# Building blocks: restrict to valid ranges instead of "any digits/letters"
# ---------------------------------------------------------------------------
MONTH_NAME = (
    r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|June?|July?|"
    r"Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
)
MM = r"(?:0?[1-9]|1[0-2])"
DD = r"(?:0?[1-9]|[12]\d|3[01])"
YYYY = r"(?:19|20)\d{2}"
YY = r"\d{2}"
ORD = r"(?:st|nd|rd|th)?"

# Left/right guards: don't match in the middle of longer numbers, IDs, versions,
# IP addresses, etc.  (Trailing "." is allowed so sentence-ending dates match.)
L = r"(?<![\w/.-])"
R = r"(?![\w/-])(?!\.\d)"

# ---------------------------------------------------------------------------
# Dates (compile with re.IGNORECASE because of month names)
# ---------------------------------------------------------------------------
date_regex = [
    # ISO / year-first with a CONSISTENT separator (backreference \1)
    rf"{L}{YYYY}([-/.]){MM}\1{DD}{R}",  # 2020-01-31, 2020/1/31, 2020.01.31
    # US month-first or day-first numeric, consistent separator, 4-digit year
    rf"{L}{MM}([-/.]){DD}\1{YYYY}{R}",  # 01/31/2020, 1-31-2020
    rf"{L}{DD}([-/.]){MM}\1{YYYY}{R}",  # 31/01/2020
    # 2-digit year: only with / or - (dots collide with decimals/versions)
    rf"{L}{MM}([-/]){DD}\1{YY}{R}",  # 01/31/20
    # Month name first: "Jan 1, 2020", "January 1st 2020", "Sept. 5, 2020"
    rf"{L}{MONTH_NAME}\.?[ ]{DD}{ORD},?[ ]{YYYY}{R}",
    # Day first: "1 Jan 2020", "01 January 2020", "1st of March, 2020"
    rf"{L}{DD}{ORD}[ ]?(?:of[ ])?{MONTH_NAME}\.?,?[ ]{YYYY}{R}",
    # Compact "1Jan2020" / "01JAN20" (no spaces => needs real month name)
    rf"{L}{DD}{MONTH_NAME}(?:{YYYY}|{YY}){R}",
    # Month + year only, requires a 4-digit year: "March 2020"
    rf"{L}{MONTH_NAME}\.?,?[ ]{YYYY}{R}",
]
date_regex = [f"(?i:{p})" for p in date_regex]  # case-insensitive month names, no external flags needed
# Optional, noisy: YYYYMMDD with valid ranges (still FP-prone on IDs)
date_compact_regex = [rf"(?<!\d){YYYY}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])(?!\d)"]

# ---------------------------------------------------------------------------
# SSN: exclude area 000/666/9xx, group 00, serial 0000
# ---------------------------------------------------------------------------
socsec_regex = [
    r"(?<![\d-])(?!000|666|9\d\d)\d{3}([- ])(?!00)\d{2}\1(?!0000)\d{4}(?![\d-])",
]

# ---------------------------------------------------------------------------
# US phone (NANP): area code and exchange can't start with 0 or 1.
# Requires separators or parentheses, so bare 10-digit IDs don't match.
# ---------------------------------------------------------------------------
phone_regex = [
    r"(?<![\w-])(?:\+?1[-.\s]?)?"
    r"(?:\([2-9]\d{2}\)[-.\s]?|[2-9]\d{2}[-.\s])"
    r"[2-9]\d{2}[-.\s]\d{4}(?![\d-])",
]
# Optional: bare 10 digits, only when a phone-ish keyword precedes it
phone_context_regex = [
    r"(?i:\b(?:phone|tel|telephone|mobile|cell|fax|call)\b)[^\d\n]{0,15}"
    r"(?:\+?1[-.\s]?)?[2-9]\d{2}[2-9]\d{2}\d{4}(?!\d)",
]

# ---------------------------------------------------------------------------
# ZIP: a bare 5-digit number is hopeless, so require context
# ---------------------------------------------------------------------------
STATES = (
    "AL|AK|AZ|AR|CA|CO|CT|DE|DC|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|"
    "MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY"
)
zip_regex = [
    # State abbreviation followed by ZIP: "Chicago, IL 60601" / "IL 60601-1234"
    rf"\b(?:{STATES})[ ,]+\d{{5}}(?:-\d{{4}})?(?!\d)",
    # ZIP+4 on its own is distinctive enough
    r"(?<![\d-])\d{5}-\d{4}(?![\d-])",
    # Keyword-led: "zip: 60601", "ZIP code 60601"
    r"(?i:\bzip(?:[ ]?code)?\b)[\s:#-]{0,5}\d{5}(?:-\d{4})?(?!\d)",
]


def compile_all(patterns):
    return [re.compile(p) for p in patterns]


all_regex = date_regex + socsec_regex + phone_regex + zip_regex  # drop-in list of strings

PATTERNS = {
    "date": compile_all(date_regex),
    "ssn": compile_all(socsec_regex),
    "phone": compile_all(phone_regex),
    "zip": compile_all(zip_regex),
}


def find_pii(text):
    """Return non-overlapping matches. Priority order resolves conflicts
    (SSN beats phone beats date beats zip)."""
    hits, taken = [], []
    for label in ("ssn", "phone", "date", "zip"):
        for rx in PATTERNS[label]:
            for m in rx.finditer(text):
                s, e = m.span()
                if any(s < te and e > ts for ts, te in taken):
                    continue
                taken.append((s, e))
                hits.append((label, m.group(0), s, e))
    return sorted(hits, key=lambda h: h[2])


if __name__ == "__main__":
    tests = [
        "Born 01/31/2020 and 2020-01-31, also Jan 1, 2020 and 1 January 2020.",
        "Version 1.2.3, IP 192.168.10.2020, ratio 10/10/2010/2011",
        "She will May be 14 in March 2020; may 3rd is not a date.",
        "SSN 123-45-6789 but not 000-12-3456 or 666-12-3456 or 987-65-4321",
        "Call (312) 555-0199, 312-555-0199, +1 312.555.0199; not 123-456-7890",
        "Order 3125550199 phone: 3125550199",
        "Chicago, IL 60601 and zip 60115 and 60601-1234; qty 12345 items",
    ]
    for t in tests:
        print(t)
        for h in find_pii(t):
            print("   ", h[0], repr(h[1]))