"""Generate fictional AHI office files: 3 offices x 3 file types (producer, provider, policy),
each office in its own format and column names, with deliberate data quality problems.

  pc01  CSV, comma, US dates (MM/DD/YYYY)
  pc02  Excel .xlsx (sheet "Data"), ISO dates
  pc03  Pipe-delimited .txt, dates as YYYYMMDD

Everything is invented: names, NPNs, NAIC codes, emails (example.com), policy numbers.
Output: data/samples/<office>/<entity>/<file>. Deterministic.
"""
import csv
import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

random.seed(42)
OUT = Path(__file__).parent / "samples"
FIRST = ["Jordan", "Priya", "Marcus", "Elena", "Sam", "Avery", "Daniel", "Grace", "Hector", "Imani", "Kenji", "Laura",
         "Mateo", "Nadia", "Owen", "Rosa", "Tariq", "Uma", "Victor", "Wendy", "Yusuf", "Zoe", "Caleb", "Dana"]
LAST = ["Ellis", "Raman", "Webb", "Ortiz", "Okafor", "Brooks", "Chen", "Delgado", "Foster", "Gupta", "Hale", "Ibarra",
        "Jensen", "Lambert", "Moreno", "Nguyen", "Osei", "Patel", "Reyes", "Sato", "Thornton", "Vance"]
PROVIDERS = [("Harborline Mutual", "Carrier", "10011"), ("Summit Peak Insurance", "Carrier", "10022"),
             ("Bluewater Specialty", "Carrier", "10033"), ("Granite State Casualty", "Carrier", "10044"),
             ("Prairie Program Underwriters", "MGA", ""), ("Coastal Excess MGU", "MGU", ""),
             ("Redwood Risk MGA", "MGA", ""), ("Lakeshore Indemnity", "Carrier", "10055")]
STATES = ["FL", "GA", "TX", "NC", "TN", "AZ", "OH", "IL"]
LOBS = ["Commercial Auto", "General Liability", "Property", "Workers Comp", "Professional Liability"]
TODAY = date(2026, 10, 27)

# shared producer pool so the same people appear at several offices (to test mastering)
POOL = []
for i in range(60):
    POOL.append({"name": f"{random.choice(FIRST)} {random.choice(LAST)}", "npn": str(17_000_000 + i * 731),
                 "agency": random.choice(["Coastal Risk Partners", "Harbor Benefits Group", "Mesa Personal Lines",
                                          "Riverbend Agency", "Pinecrest Advisors"]),
                 "state": random.choice(STATES)})

OFFICES = {
    "pc01": {"fmt": "csv", "date": lambda d: d.strftime("%m/%d/%Y")},
    "pc02": {"fmt": "xlsx", "date": lambda d: d.isoformat()},
    "pc03": {"fmt": "txt", "date": lambda d: d.strftime("%Y%m%d")},
}
COLS = {  # each office names its columns differently
    "pc01": {"producer": ["Agent Code", "Agent Name", "NPN", "Email", "Agency", "State"],
             "provider": ["Carrier Code", "Carrier Name", "Type", "NAIC", "Domicile"],
             "policy": ["Policy No", "Eff Date", "Exp Date", "Status", "LOB", "Written Premium", "Agent Code", "Carrier Code", "Risk State"]},
    "pc02": {"producer": ["ProducerID", "ProducerName", "NationalProducerNumber", "EmailAddress", "AgencyName", "LicenseState"],
             "provider": ["ProviderID", "ProviderName", "ProviderType", "NAICCode", "State"],
             "policy": ["PolicyNumber", "EffectiveDate", "ExpirationDate", "PolicyStatus", "LineOfBusiness", "Premium", "ProducerID", "ProviderID", "State"]},
    "pc03": {"producer": ["prod_cd", "prod_nm", "npn_no", "email_addr", "agcy_nm", "st"],
             "provider": ["mkt_cd", "mkt_nm", "mkt_type", "naic_cd", "dom_st"],
             "policy": ["pol_num", "eff_dt", "exp_dt", "pol_stat", "lob_desc", "wrtn_prem", "prod_cd", "mkt_cd", "risk_st"]},
}


def write(office, entity, header, rows):
    d = OUT / office / entity
    d.mkdir(parents=True, exist_ok=True)
    fmt = OFFICES[office]["fmt"]
    name = f"{office}_{entity}_20261026.{fmt}"
    if fmt == "xlsx":
        pd.DataFrame(rows, columns=header).to_excel(d / name, sheet_name="Data", index=False)
    else:
        with open(d / name, "w", newline="") as f:
            w = csv.writer(f, delimiter="," if fmt == "csv" else "|")
            w.writerow(header)
            w.writerows(rows)
    return len(rows)


summary = []
for office, spec in OFFICES.items():
    fd = spec["date"]
    # producers: 25 per office from the shared pool, a few with problems
    prods = random.sample(POOL, 25)
    prow = []
    for i, p in enumerate(prods):
        npn = p["npn"]
        email = p["name"].lower().replace(" ", ".") + "@example.com"
        if i == 3:
            npn = ""                      # missing NPN -> warn
        if i == 7:
            npn = "12AB"                  # bad NPN format -> warn
        if i == 11:
            email = "not-an-email"        # bad email -> warn
        prow.append([f"{office.upper()}-P{100 + i}", p["name"], npn, email, p["agency"], p["state"]])
    prow.append(prow[0][:])               # duplicate row -> removed as duplicate
    summary.append((office, "producer", write(office, "producer", COLS[office]["producer"], prow)))

    # providers: 6 per office
    provs = random.sample(PROVIDERS, 6)
    vrow = [[f"{office.upper()}-M{10 + i}", n, t, c, random.choice(STATES)] for i, (n, t, c) in enumerate(provs)]
    vrow[2][2] = "Insurer"                # invalid provider type -> drop
    summary.append((office, "provider", write(office, "provider", COLS[office]["provider"], vrow)))

    # policies: 120 per office
    pol = []
    for i in range(120):
        eff = TODAY - timedelta(days=random.randint(0, 700))
        exp = eff + timedelta(days=365)
        status = random.choices(["Active", "Cancelled", "Expired"], [0.8, 0.1, 0.1])[0]
        if exp <= TODAY and status == "Active":
            status = "Expired"
        prem = round(random.uniform(1500, 95000), 2)
        prod = random.choice(prow[:-1])[0]
        prov = random.choice(vrow)[0]
        row = [f"{office.upper()}-{2026000 + i}", fd(eff), fd(exp), status, random.choice(LOBS), f"{prem:.2f}", prod, prov,
               random.choice(STATES)]
        if i == 5:
            row[0] = ""                    # missing policy number -> drop
        if i == 9:
            row[2] = fd(eff - timedelta(days=30))   # expiration before effective -> drop
        if i == 13:
            row[5] = "-500.00"             # negative premium -> drop
        if i == 17:
            row[6] = f"{office.upper()}-P999"        # unknown producer -> warn (orphan)
        if i == 21:
            row[8] = "ZZ"                  # invalid state -> drop
        pol.append(row)
    summary.append((office, "policy", write(office, "policy", COLS[office]["policy"], pol)))

for s in summary:
    print(*s)
