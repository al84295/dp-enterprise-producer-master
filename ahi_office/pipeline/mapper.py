"""Mapping process: conform office files with different structures to the AHI standard model.

Every bronze row carries a schema signature (a fingerprint of its column names). For each
(office, entity, signature) the pipeline needs an APPROVED mapping before the rows can reach silver:

  1. The office's default mapping in config/offices.yml counts as approved.
  2. Other approved mappings live in config/mappings/<office>_<entity>_<signature>.yml (reviewed like code).
  3. A new structure gets a PROPOSED mapping, generated here from column names and the values in the
     columns. Its rows are held in bronze (not lost, not quarantined) until a person approves it.

Run on a single file:  python3 pipeline/mapper.py --file path.csv --office pc04 --entity policy [--sep ,]
"""
import argparse
import difflib
import hashlib
import re
from pathlib import Path

import pandas as pd
import yaml

META_PREFIX = "_"
REQUIRED = {
    "producer": ["source_key", "producer_name"],
    "provider": ["source_key", "provider_name", "provider_type"],
    "policy": ["policy_number", "term_effective_date", "expiration_date", "status", "line_of_business",
               "written_premium", "producer_key", "provider_key", "risk_state"],
}
SYNONYMS = {
    "producer": {
        "source_key": ["agent code", "producer id", "producer code", "prod cd", "agent id", "producer number", "agent number"],
        "producer_name": ["agent name", "producer name", "prod nm", "full name", "name"],
        "npn": ["npn", "national producer number", "npn no", "npn number"],
        "email": ["email", "email address", "e mail", "email addr"],
        "agency_name": ["agency", "agency name", "agcy nm", "office name", "firm"],
        "state": ["state", "license state", "licence state", "st", "resident state"],
    },
    "provider": {
        "source_key": ["carrier code", "provider id", "market code", "mkt cd", "carrier id", "company code"],
        "provider_name": ["carrier name", "provider name", "market name", "mkt nm", "company name", "insurer"],
        "provider_type": ["type", "provider type", "market type", "mkt type", "carrier type"],
        "naic_code": ["naic", "naic code", "naic cd", "naic number"],
        "state": ["domicile", "state", "dom st", "domicile state"],
    },
    "policy": {
        "policy_number": ["policy no", "policy number", "pol num", "policy id", "policy #", "pol no"],
        "term_effective_date": ["eff date", "effective date", "eff dt", "inception date", "start date", "effective"],
        "expiration_date": ["exp date", "expiration date", "exp dt", "expiry date", "end date", "expiration"],
        "status": ["status", "policy status", "pol stat"],
        "line_of_business": ["lob", "line of business", "lob desc", "coverage line", "line"],
        "written_premium": ["written premium", "premium", "wrtn prem", "gross premium", "written prem"],
        "producer_key": ["agent code", "producer id", "prod cd", "producer code", "agent id"],
        "provider_key": ["carrier code", "provider id", "mkt cd", "market code", "carrier id"],
        "risk_state": ["risk state", "state", "risk st", "location state"],
    },
}
DATE_FORMATS = ["%m/%d/%Y", "%Y-%m-%d", "%Y%m%d", "%d/%m/%Y", "%m-%d-%Y", "%Y/%m/%d", "%m/%d/%y"]


def snake(name):
    s = re.sub(r"[^0-9a-zA-Z]+", "_", str(name)).strip("_")
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()


def norm(s):
    return " ".join(snake(s).split("_"))


def signature(columns):
    cols = sorted(snake(c) for c in columns if not str(c).startswith(META_PREFIX))
    return hashlib.sha1("|".join(cols).encode()).hexdigest()[:12]


# ---------------------------------------------------------------- value checks (share of sample values that fit)
def _share(values, pred):
    v = [x for x in values if x not in (None, "") and str(x).strip()]
    return sum(1 for x in v if pred(str(x).strip())) / len(v) if v else 0.0


def value_score(std, values, ref):
    states = set(ref["states"])
    checks = {
        "npn": lambda x: bool(re.fullmatch(r"\d{6,10}", x)),
        "email": lambda x: "@" in x,
        "state": lambda x: x.upper() in states,
        "risk_state": lambda x: x.upper() in states,
        "naic_code": lambda x: bool(re.fullmatch(r"\d{5}", x)),
        "provider_type": lambda x: x in ref["provider_types"],
        "status": lambda x: x in ref["policy_status"],
        "line_of_business": lambda x: x in ref["lines_of_business"],
        "written_premium": lambda x: bool(re.fullmatch(r"-?[\d,]+(\.\d+)?", x)),
        "term_effective_date": lambda x: best_date_format([x]) is not None,
        "expiration_date": lambda x: best_date_format([x]) is not None,
    }
    if std in checks:
        return _share(values, checks[std])
    return None


def best_date_format(values):
    vals = [str(v).strip() for v in values if str(v).strip()][:50]
    best, score = None, 0.0
    for f in DATE_FORMATS:
        ok = sum(1 for v in vals if not pd.isna(pd.to_datetime(v, format=f, errors="coerce")))
        if vals and ok / len(vals) > score:
            best, score = f, ok / len(vals)
    return best if score >= 0.9 else None


def name_score(src, std, entity):
    s = norm(src)
    best = max(difflib.SequenceMatcher(None, s, syn).ratio() for syn in SYNONYMS[entity][std] + [norm(std)])
    if s in SYNONYMS[entity][std]:
        best = 1.0
    return best


def propose(df, entity, ref, threshold=0.75):
    """Propose {source column: standard column} for a DataFrame of one file. Returns a mapping document."""
    cols = [c for c in df.columns if not str(c).startswith(META_PREFIX)]
    sample = {c: df[c].dropna().astype(str).head(200).tolist() for c in cols}
    cand = []
    for std in SYNONYMS[entity]:
        for c in cols:
            ns, vs = name_score(c, std, entity), value_score(std, sample[c], ref)
            score = ns if vs is None else 0.55 * ns + 0.45 * vs
            cand.append((round(score, 3), std, c))
    mapping, conf, used = {}, {}, set()
    for score, std, c in sorted(cand, reverse=True):           # greedy one-to-one
        if std in conf or c in used or score < 0.45:
            continue
        mapping[c], conf[std] = std, score
        used.add(c)
    date_col = next((c for c, s in mapping.items() if s == "term_effective_date"), None)
    doc = {
        "entity": entity,
        "signature": signature(cols),
        "status": "proposed",
        "columns": mapping,
        "confidence": conf,
        "date_format": best_date_format(sample[date_col]) if date_col else None,
        "missing_required": [r for r in REQUIRED[entity] if r not in conf],
        "low_confidence": [s for s, v in conf.items() if v < threshold],
        "unmapped_source_columns": [c for c in cols if c not in mapping],
    }
    doc["auto_approvable"] = not doc["missing_required"] and not doc["low_confidence"] and (entity != "policy" or doc["date_format"])
    return doc


# ---------------------------------------------------------------- registry
class MappingRegistry:
    """Approved mappings: each office's default (offices.yml) plus reviewed files in config/mappings/."""

    def __init__(self, cfg, mappings_dir, also_read=()):
        """mappings_dir: where proposals are written and approvals read. also_read: more approved-mapping folders
        (for example config/mappings in the repo, deployed with the bundle)."""
        self.cfg = cfg
        self.dir = Path(mappings_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.read_dirs = [self.dir] + [Path(d) for d in also_read]

    def path(self, office, entity, sig, status):
        return self.dir / f"{office}_{entity}_{sig}.{status}.yml"

    def approved(self, office, entity, sig):
        oc = self.cfg["offices"].get(office, {})
        default = oc.get("columns", {}).get(entity)
        if default and signature(default.keys()) == sig:
            return {"columns": {snake(k): v for k, v in default.items()}, "date_format": oc.get("date_format"),
                    "source": "offices.yml"}
        for d in self.read_dirs:
            p = d / f"{office}_{entity}_{sig}.approved.yml"
            if p.exists():
                doc = yaml.safe_load(p.read_text())
                return {"columns": {snake(k): v for k, v in doc["columns"].items()}, "date_format": doc.get("date_format"),
                        "source": p.name}
        return None

    def save_proposal(self, office, entity, doc):
        doc = dict(doc, office=office)
        p = self.path(office, entity, doc["signature"], "proposed")
        header = ("# PROPOSED mapping. Review the columns and date_format, then rename this file to\n"
                  f"# {office}_{entity}_{doc['signature']}.approved.yml (via pull request) to release the held rows.\n")
        p.write_text(header + yaml.safe_dump(doc, sort_keys=False))
        return p


def main():
    a = argparse.ArgumentParser()
    a.add_argument("--file", required=True)
    a.add_argument("--office", required=True)
    a.add_argument("--entity", required=True, choices=list(SYNONYMS))
    a.add_argument("--sep", default=None, help="Delimiter for text files (default: detect)")
    a.add_argument("--config", default=str(Path(__file__).resolve().parent.parent / "config" / "offices.yml"))
    args = a.parse_args()
    cfg = yaml.safe_load(open(args.config))
    f = Path(args.file)
    df = (pd.read_excel(f, dtype=str, keep_default_na=False) if f.suffix.lower() in (".xlsx", ".xls")
          else pd.read_csv(f, dtype=str, keep_default_na=False, sep=args.sep, engine="python"))
    print(yaml.safe_dump(propose(df, args.entity, cfg["reference"]), sort_keys=False))


if __name__ == "__main__":
    main()
