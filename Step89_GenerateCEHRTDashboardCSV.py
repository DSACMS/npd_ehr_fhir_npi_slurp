#!/usr/bin/env python3
"""
Step89_GenerateCEHRTDashboardCSV.py

Generates a CSV file (CEHRT_FHIR_Report.csv) with compliance results for each CEHRT vendor.
This file is used as input for the dashboard markdown generator.

- Reads prod_data/list_sources_summary.csv for vendor info.
- Reads data/output_data/enriched_endpoints.csv for endpoint compliance.
- Reads data/output_data/normalized_csv_files/org_to_npi.csv for partial compliance.
- Aggregates compliance per vendor.

Columns: Vendor, Reachable, Has ONPI, HTTPS ORG URL, Findable Metadata, Findable SMART, Findable OpenAPI Docs, Findable OpenAPI JSON, Findable Swagger, Findable Swagger JSON
"""

import csv
import os
from urllib.parse import urlparse
import re
import requests

CHECKS = [
    ("Reachable", "reachable"),
    ("Has ONPI", "has_onpi"),
    ("HTTPS ORG URL", "https_org_url"),
    ("Findable Metadata", "capability_url"),
    ("Findable SMART", "smart_url"),
    ("Findable OpenAPI Docs", "openapi_docs_url"),
    ("Findable OpenAPI JSON", "openapi_json_url"),
    ("Findable Swagger", "swagger_url"),
    ("Findable Swagger JSON", "swagger_json_url"),
]

def get_base_domain(url):
    try:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        return url

def load_vendor_mapping(list_sources_path):
    mapping = {}
    with open(list_sources_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            list_source = row.get("list_source", "").strip()
            vendor = row.get("certified_api_developer_name", "").strip()
            if list_source.startswith("http"):
                base = get_base_domain(list_source)
                mapping[base] = vendor if vendor else "Unknown"
    return mapping

def is_valid_npi(npi_value):
    return bool(re.match(r'^\d{10}$', npi_value.strip()))

def is_valid_https_url(url):
    return url.strip().startswith('https://')

def is_domain_responsive(base_domain):
    try:
        response = requests.get(base_domain, timeout=10, allow_redirects=True)
        return 200 <= response.status_code < 500
    except Exception:
        return False

def check_reachable(row):
    for col in [
        "capability_url", "smart_url", "openapi_docs_url",
        "openapi_json_url", "swagger_url", "swagger_json_url"
    ]:
        if row.get(col, "").startswith("http"):
            return True
    return False

def check_has_onpi(row):
    npi = row.get("npi", "").strip()
    return is_valid_npi(npi)

def check_https_org_url(row):
    return row.get("org_fhir_url", "").strip().startswith("https://")

def check_endpoint_found(row, col):
    return row.get(col, "").startswith("http")

def aggregate_vendor_compliance(enriched_path, org_to_npi_path, vendor_map):
    # 1. Parse org_to_npi.csv and collect per-vendor org info
    org_to_npi = {}
    with open(org_to_npi_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            org_id = row.get("org_id", "").strip()
            npi_value = row.get("npi_value", "").strip()
            base = get_base_domain(org_id)
            vendor = vendor_map.get(base, "Unknown")
            if vendor not in org_to_npi:
                org_to_npi[vendor] = []
            org_to_npi[vendor].append((org_id, npi_value))

    # 2. Parse enriched_endpoints.csv
    org_in_enriched = {}
    with open(enriched_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            org_url = row.get("org_fhir_url", "").strip()
            base = get_base_domain(org_url)
            vendor = vendor_map.get(base, "Unknown")
            if vendor not in org_in_enriched:
                org_in_enriched[vendor] = set()
            org_in_enriched[vendor].add(org_url)

    # 3. Aggregate compliance
    vendor_results = {}

    # Vendors with orgs in enriched_endpoints.csv (normal logic)
    with open(enriched_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            org_url = row.get("org_fhir_url", "").strip()
            base = get_base_domain(org_url)
            vendor = vendor_map.get(base, "Unknown")
            if vendor not in vendor_results:
                vendor_results[vendor] = {c[0]: False for c in CHECKS}

            checks = {
                "Reachable": check_reachable(row),
                "Has ONPI": check_has_onpi(row),
                "HTTPS ORG URL": check_https_org_url(row),
                "Findable Metadata": check_endpoint_found(row, "capability_url"),
                "Findable SMART": check_endpoint_found(row, "smart_url"),
                "Findable OpenAPI Docs": check_endpoint_found(row, "openapi_docs_url"),
                "Findable OpenAPI JSON": check_endpoint_found(row, "openapi_json_url"),
                "Findable Swagger": check_endpoint_found(row, "swagger_url"),
                "Findable Swagger JSON": check_endpoint_found(row, "swagger_json_url"),
            }
            for k, v in checks.items():
                if v:
                    vendor_results[vendor][k] = True

    # Vendors with orgs in org_to_npi.csv but not in enriched_endpoints.csv
    for vendor in org_to_npi:
        if vendor not in vendor_results:
            reachable = False
            has_onpi = False
            https_org_url = False
            for org_id, npi_value in org_to_npi[vendor]:
                base_domain = get_base_domain(org_id)
                if is_domain_responsive(base_domain):
                    reachable = True
                if is_valid_npi(npi_value):
                    has_onpi = True
                if is_valid_https_url(org_id):
                    https_org_url = True
            vendor_results[vendor] = {c[0]: False for c in CHECKS}
            vendor_results[vendor]["Reachable"] = reachable
            vendor_results[vendor]["Has ONPI"] = has_onpi
            vendor_results[vendor]["HTTPS ORG URL"] = https_org_url

    # Vendors in vendor_map but not in either file: all fail
    for vendor in set(vendor_map.values()):
        if vendor not in vendor_results:
            vendor_results[vendor] = {c[0]: False for c in CHECKS}

    return vendor_results

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    list_sources_path = os.path.join(base_dir, "prod_data", "list_sources_summary.csv")
    enriched_path = os.path.join(base_dir, "data", "output_data", "enriched_endpoints.csv")
    org_to_npi_path = os.path.join(base_dir, "data", "output_data", "normalized_csv_files", "org_to_npi.csv")
    output_csv = os.path.join(base_dir, "CEHRT_FHIR_Report.csv")

    print("Loading vendor mapping from prod_data/list_sources_summary.csv...")
    vendor_map = load_vendor_mapping(list_sources_path)
    print(f"Loaded {len(vendor_map)} vendor base domains.")

    print("Parsing org_to_npi.csv for partial compliance info...")
    with open(org_to_npi_path, newline='', encoding='utf-8') as f:
        org_to_npi_count = sum(1 for _ in f) - 1
    print(f"Found {org_to_npi_count} org_id rows in org_to_npi.csv.")

    print("Parsing enriched_endpoints.csv for endpoint compliance info...")
    with open(enriched_path, newline='', encoding='utf-8') as f:
        enriched_count = sum(1 for _ in f) - 1
    print(f"Found {enriched_count} org_fhir_url rows in enriched_endpoints.csv.")

    print("Aggregating compliance results per vendor...")
    vendor_results = aggregate_vendor_compliance(enriched_path, org_to_npi_path, vendor_map)
    print(f"Aggregated compliance for {len(vendor_results)} vendors.")

    print("Writing dashboard CSV output...")
    with open(output_csv, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        header = ["Vendor"] + [c[0] for c in CHECKS]
        writer.writerow(header)
        # Sort: all-green vendors first, then alphabetically
        def is_all_green(results):
            return all(results[c[0]] for c in CHECKS)
        sorted_vendors = sorted(
            vendor_results.items(),
            key=lambda x: (not is_all_green(x[1]), x[0].lower())
        )
        for vendor, results in sorted_vendors:
            row = [vendor] + [str(results[c[0]]) for c in CHECKS]
            writer.writerow(row)

    print(f"Dashboard CSV written to {output_csv}")

if __name__ == "__main__":
    main()
