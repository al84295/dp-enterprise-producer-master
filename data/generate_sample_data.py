"""Generate a fictional producer master data sample set.

All people, agencies, NPNs, emails and phone numbers are invented for testing.
Emails use example.com and phones use the 555-01xx fictional range.

Output (next to this script):
  producer_golden.csv          ~150 steward-managed golden records
  producer_source_records.csv  ~330 records from four source systems
  producer_change_log.csv      seed audit entries

Run: python3 generate_sample_data.py   (deterministic: same output every run)
"""
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

random.seed(20261023)
OUT = Path(__file__).parent
NOW = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)

FIRST = ["Jordan", "Priya", "Marcus", "Elena", "Sam", "Avery", "Daniel", "Grace", "Hector", "Imani",
         "Kenji", "Laura", "Mateo", "Nadia", "Owen", "Rosa", "Tariq", "Uma", "Victor", "Wendy",
         "Yusuf", "Zoe", "Caleb", "Dana", "Ethan", "Fatima", "Gavin", "Hana", "Isaac", "Julia",
         "Kofi", "Leah", "Miguel", "Nina", "Oscar", "Paula", "Quinn", "Rahul", "Sofia", "Trevor"]
LAST = ["Ellis", "Raman", "Webb", "Ortiz", "Okafor", "Brooks", "Chen", "Delgado", "Foster", "Gupta",
        "Hale", "Ibarra", "Jensen", "Kowalski", "Lambert", "Moreno", "Nguyen", "Osei", "Patel", "Quist",
        "Reyes", "Sato", "Thornton", "Underwood", "Vance", "Whitaker", "Xu", "Yamada", "Zimmer", "Abbott"]
AGENCIES = [  # fictional
    ("Coastal Risk Partners", "Retail", "Tampa FL", ["FL", "GA"]),
    ("Harbor Benefits Group", "Retail", "Boston MA", ["MA", "NH", "RI"]),
    ("Pinecrest Insurance Advisors", "Retail", "Charlotte NC", ["NC", "SC", "VA"]),
    ("Lakeside Commercial Insurance", "Retail", "Cleveland OH", ["OH", "PA", "MI"]),
    ("Mesa Personal Lines", "Retail", "Phoenix AZ", ["AZ", "NM", "NV"]),
    ("Riverbend Agency", "Retail", "Nashville TN", ["TN", "KY", "AL"]),
    ("Summit Wholesale", "Specialty", "Chicago IL", ["IL", "IN", "WI"]),
    ("Delta Program Underwriters", "Specialty", "Dallas TX", ["TX", "OK", "LA"]),
    ("Northstar E&S Brokerage", "Specialty", "Minneapolis MN", ["MN", "IA", "ND", "SD"]),
    ("Granite Professional Lines", "Specialty", "Denver CO", ["CO", "UT", "WY"]),
    ("Bayview Excess Markets", "Specialty", "San Francisco CA", ["CA", "OR", "WA"]),
    ("Keystone Transportation Programs", "Specialty", "Atlanta GA", ["GA", "FL", "AL", "MS"]),
]
SOURCES = ["AMS-East", "AMS-West", "CRM", "ProgramAdmin"]
PREFIX = {"AMS-East": "AE", "AMS-West": "AW", "CRM": "C", "ProgramAdmin": "P"}


def ts(days_ago):
    return (NOW - timedelta(days=days_ago, minutes=random.randint(0, 1440))).strftime("%Y-%m-%d %H:%M:%S")


def email_for(first, last, style=0):
    return [f"{first}.{last}", f"{first[0]}{last}", f"{first}{last[0]}"][style].lower() + "@example.com"


# ---------- golden records ----------
golden, used = [], set()
n = 0
while len(golden) < 150:
    first, last = random.choice(FIRST), random.choice(LAST)
    if (first, last) in used:
        continue
    used.add((first, last))
    n += 1
    agency, segment, office, states = random.choice(AGENCIES)
    lic = sorted(random.sample(states, k=random.randint(1, len(states))))
    rec = {
        "producer_id": f"PRD-{n:06d}",
        "producer_name": f"{first} {last}",
        "npn": str(random.randint(10_000_000, 19_999_999)),
        "email": email_for(first, last),
        "phone": f"555-01{random.randint(0, 99):02d}",
        "agency_name": agency,
        "segment": segment,
        "office": office,
        "licensed_states": ",".join(lic),
        "status": "Active" if random.random() < 0.88 else "Inactive",
        "steward_notes": "",
        "version": 1,
        "updated_by": "seed",
        "updated_at": ts(random.randint(30, 400)),
    }
    # realistic data-quality gaps for stewards to fix
    r = random.random()
    if r < 0.09:
        rec["npn"] = ""
        rec["steward_notes"] = "NPN missing: confirm with licensing"
    elif r < 0.17:
        rec["email"] = ""
    elif r < 0.22:
        rec["phone"] = ""
    golden.append(rec)

# ---------- source records ----------
def variant(name):
    first, last = name.split(" ", 1)
    return random.choice([name, f"{first[0]}. {last}", f"{first} {random.choice('ABCDEJLMR')}. {last}",
                          name.upper(), f"{last}, {first}"])


source = []
key_no = {s: 1000 for s in SOURCES}


def add_source(sys, name, npn, email, agency, pid, conf, status, days):
    key_no[sys] += random.randint(1, 37)
    reviewed = status in ("Matched", "Rejected")
    loaded = NOW - timedelta(days=days, minutes=random.randint(0, 1440))
    fmt = "%Y-%m-%d %H:%M:%S"
    source.append({
        "source_system": sys,
        "source_key": f"{PREFIX[sys]}-{key_no[sys]}",
        "producer_name": name, "npn": npn, "email": email, "agency_name": agency,
        "matched_producer_id": pid, "match_confidence": "" if conf is None else f"{conf:.2f}",
        "match_status": status,
        "reviewed_by": "seed" if reviewed else "", "reviewed_at": (loaded + timedelta(hours=random.randint(2, 48))).strftime(fmt) if reviewed else "",
        "loaded_at": loaded.strftime(fmt),
    })


for g in golden:
    # every golden record has at least one confirmed source
    add_source(random.choice(SOURCES[:2]), g["producer_name"], g["npn"], g["email"], g["agency_name"],
               g["producer_id"], round(random.uniform(0.97, 1.0), 2), "Matched", random.randint(20, 300))
    # about 60% appear in a second system, some still awaiting review
    if random.random() < 0.6:
        sys = random.choice(SOURCES[2:])
        npn = g["npn"] if random.random() < 0.6 else ""
        email = g["email"] if random.random() < 0.5 else email_for(*g["producer_name"].split(" ", 1), style=random.randint(1, 2))
        pending = random.random() < 0.35
        conf = round(random.uniform(0.95, 0.99), 2) if npn else round(random.uniform(0.62, 0.9), 2)
        add_source(sys, variant(g["producer_name"]), npn, email, g["agency_name"], g["producer_id"],
                   conf, "Proposed" if pending else "Matched", random.randint(1, 60) if pending else random.randint(20, 200))

# a few rejected matches (different people with similar names)
for g in random.sample(golden, 6):
    first, last = g["producer_name"].split(" ", 1)
    add_source("CRM", f"{random.choice(FIRST)} {last}", str(random.randint(20_000_000, 29_999_999)), "",
               random.choice(AGENCIES)[0], "", None, "Rejected", random.randint(10, 90))

# brand-new producers not yet in the golden set
for _ in range(14):
    first, last = random.choice(FIRST), random.choice(LAST)
    if (first, last) in used:
        continue
    used.add((first, last))
    agency = random.choice(AGENCIES)[0]
    add_source(random.choice(SOURCES), f"{first} {last}",
               str(random.randint(20_000_000, 29_999_999)) if random.random() < 0.7 else "",
               email_for(first, last) if random.random() < 0.8 else "", agency, "", None, "Unmatched",
               random.randint(1, 30))

# ---------- change log ----------
log = []
for g in golden:
    log.append({"change_id": f"seed-{g['producer_id']}", "producer_id": g["producer_id"], "action": "create",
                "field": "", "old_value": "", "new_value": "", "reason": "Initial load from AMS",
                "changed_by": "seed", "changed_at": g["updated_at"]})


def write(name, rows):
    with open(OUT / name, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"{name}: {len(rows)} rows")


write("producer_golden.csv", golden)
write("producer_source_records.csv", source)
write("producer_change_log.csv", log)
