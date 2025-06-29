#!/usr/bin/env python3
"""
hti1_probe.py  –  Probe FHIR base URLs for HTI-1 organisational metadata.

INPUT
  1. Lantern-style CSV whose only required column is  "url"
     (the column may actually be called "resolves" in some Lantern exports)

OUTPUT
  A new CSV with columns
      url, npi, org_name, status, failure_reason
"""

import sys, csv, json, re, time
from urllib.parse import urljoin

import pandas as pd
import requests
from tqdm import tqdm

# ---------- configuration ----------
TIMEOUT = 15               # seconds per request
HEADERS = {"Accept": "application/fhir+json"}
NPI_SYSTEMS = {
    "http://hl7.org/fhir/sid/us-npi",
    "2.16.840.1.113883.4.6",          # OID form
}
# ------------------------------------

def safe_get(url):
    """GET URL and return (status, seconds, json_or_None, text)"""
    t0 = time.time()
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        elapsed = time.time() - t0
        try:
            js = r.json()
        except Exception:
            js = None
        return r.status_code, elapsed, js, r.text[:120]
    except Exception as e:
        return None, None, None, str(e)

def find_npi_in_resource(resource):
    """Return (npi, org_name) tuple or (None, None)."""
    if not isinstance(resource, dict):
        return (None, None)
    # 1) look for identifier with NPI system
    for ident in resource.get("identifier", []):
        if ident.get("system") in NPI_SYSTEMS:
            return ident.get("value"), resource.get("name")
    # 2) heuristic: any 10-digit number labelled NPI
    npi_regex = re.compile(r"\b\d{10}\b")
    for k, v in resource.items():
        if isinstance(v, str) and ("npi" in k.lower() or "npi" in v.lower()):
            m = npi_regex.search(v)
            if m:
                return m.group(), resource.get("name")
    return (None, None)

def probe_base(base_url):
    base_url = base_url.rstrip("/") + "/"   # normalise
    outcome = {
        "url": base_url,
        "npi": "",
        "org_name": "",
        "status": "failure",
        "failure_reason": "",
    }

    # STEP 1 – /metadata
    meta_url = urljoin(base_url, "metadata")
    status, _, cap, txt = safe_get(meta_url)
    if status != 200 or not cap or cap.get("resourceType") != "CapabilityStatement":
        outcome["failure_reason"] = f"/metadata status {status or 'timeout'}"
        return outcome

    # Try to find NPI directly in CapabilityStatement
    npi, org_name = find_npi_in_resource(cap)
    if npi:
        outcome.update({"npi": npi, "org_name": org_name or "", "status": "success"})
        return outcome

    # STEP 2 – search Endpoint
    ep_search = urljoin(base_url, "Endpoint?_count=10")
    status, _, bundle, txt = safe_get(ep_search)
    if status != 200 or not bundle or bundle.get("resourceType") != "Bundle":
        outcome["failure_reason"] = f"/Endpoint search status {status or 'timeout'}"
        return outcome

    for entry in bundle.get("entry", []):
        ep = entry.get("resource", {})
        if ep.get("resourceType") != "Endpoint":
            continue
        # Look for managingOrganization reference
        org_ref = ep.get("managingOrganization", {}).get("reference")
        if not org_ref:
            continue
        # STEP 3 – follow Organization link
        org_url = urljoin(base_url, org_ref)
        status, _, org, txt = safe_get(org_url)
        if status != 200 or not org or org.get("resourceType") != "Organization":
            outcome["failure_reason"] = f"Org {org_ref} status {status or 'timeout'}"
            return outcome
        npi, org_name = find_npi_in_resource(org)
        if npi:
            outcome.update({"npi": npi, "org_name": org_name or "", "status": "success"})
            return outcome
        else:
            outcome["failure_reason"] = "Organization exists, but no NPI identifier"
            return outcome

    outcome["failure_reason"] = "No managingOrganization in any Endpoint"
    return outcome

def main(src_csv, dst_csv):
    df = pd.read_csv(src_csv)
    # Lantern sometimes labels the column "resolves" instead of "url"
    if "url" not in df.columns and "resolves" in df.columns:
        df.rename(columns={"resolves": "url"}, inplace=True)

    results = []
    for url in tqdm(df["url"], desc="Checking"):
        results.append(probe_base(url))

    pd.DataFrame(results).to_csv(dst_csv, index=False)
    print(f"\nWritten report to {dst_csv}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("USAGE:  python slurp.py <endpoints.csv> <report.csv>")
    main(sys.argv[1], sys.argv[2])
