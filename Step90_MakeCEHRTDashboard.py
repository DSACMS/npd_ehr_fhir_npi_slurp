#!/usr/bin/env python3
"""
Step90_MakeCEHRTDashboard.py

Generates a markdown dashboard (CEHRT_FHIR_Report.md) listing CEHRT vendors in order of their compliance
with a scrappable FHIR ecosystem, using shield.io badges for each compliance check.

- Reads prod_data/list_sources_summary.csv for vendor info.
- Reads data/output_data/enriched_endpoints.csv for endpoint compliance.
- Aggregates compliance per vendor.
- Outputs CEHRT_FHIR_Report.md in the project root.

See AI_Instructions/CreateEHrVendorMarkdownReport.md for details.
"""

import csv
import os
from urllib.parse import urlparse

# Compliance checks and their markdown column names
CHECKS = [
    ("Reachable", "reachable"),
    ("Has ONPI", "has_onpi"),
    ("HTTPS ORG URL", "https_org_url"),
    ("Findable Capabilities", "capability_url"),
    ("Findable SMART", "smart_url"),
    ("Findable OpenAPI Docs", "openapi_docs_url"),
    ("Findable OpenAPI JSON", "openapi_json_url"),
    ("Findable Swagger", "swagger_url"),
    ("Findable Swagger JSON", "swagger_json_url"),
]

# Shield.io badge templates
BADGES = {
    True: "https://img.shields.io/badge/{label}-green?style=for-the-badge",
    False: "https://img.shields.io/badge/{label}-red?style=for-the-badge"
}

def get_base_domain(url):
    """Extract scheme://netloc from a URL."""
    try:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        return url

def load_vendor_mapping(list_sources_path):
    """
    Returns a dict mapping base domain (scheme://netloc) to certified_api_developer_name.
    If multiple list_sources share a domain, the last one wins.
    """
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

def is_https_url(url):
    return url.startswith("https://")

def is_valid_url(url):
    return url.startswith("http://") or url.startswith("https://")

def check_reachable(row):
    # If any of the endpoint checks is a valid URL, consider it reachable
    for col in [
        "capability_url", "smart_url", "openapi_docs_url",
        "openapi_json_url", "swagger_url", "swagger_json_url"
    ]:
        if is_valid_url(row.get(col, "")):
            return True
    return False

def check_has_onpi(row):
    # If NPI is a non-empty string of digits, consider it present
    npi = row.get("npi", "").strip()
    return npi.isdigit() and len(npi) > 0

def check_https_org_url(row):
    # If org_fhir_url is https, consider it compliant
    return is_https_url(row.get("org_fhir_url", ""))

def check_endpoint_found(row, col):
    # If the value is a valid URL, consider it found
    return is_valid_url(row.get(col, ""))

# Updated to accept org_to_npi_path as a parameter and implement correct logic
def aggregate_vendor_compliance(enriched_path, org_to_npi_path, vendor_map):
    """
    Returns a dict: vendor_name -> {check_name: bool, ...}
    Implements logic:
    - For orgs in org_to_npi.csv but not in enriched_endpoints.csv, use Step50 logic for first three columns.
    - For orgs in enriched_endpoints.csv, use normal logic.
    - If vendor has no org_id in either: all fail.
    """
    import re
    import requests

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

    # 1. Parse org_to_npi.csv and collect per-vendor org info
    org_to_npi = {}
    org_to_npi_rows = []
    with open(org_to_npi_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            org_id = row.get("org_id", "").strip()
            npi_value = row.get("npi_value", "").strip()
            base = get_base_domain(org_id)
            vendor = vendor_map.get(base, "Unknown")
            org_to_npi_rows.append((vendor, org_id, npi_value))
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

    # First, handle vendors with orgs in enriched_endpoints.csv (normal logic)
    with open(enriched_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            org_url = row.get("org_fhir_url", "").strip()
            base = get_base_domain(org_url)
            vendor = vendor_map.get(base, "Unknown")
            if vendor not in vendor_results:
                vendor_results[vendor] = {c[0]: False for c in CHECKS}
                vendor_results[vendor]["_count"] = 0  # For sorting

            # Evaluate all checks for this row
            checks = {
                "Reachable": check_reachable(row),
                "Has ONPI": check_has_onpi(row),
                "HTTPS ORG URL": check_https_org_url(row),
                "Findable Capabilities": check_endpoint_found(row, "capability_url"),
                "Findable SMART": check_endpoint_found(row, "smart_url"),
                "Findable OpenAPI Docs": check_endpoint_found(row, "openapi_docs_url"),
                "Findable OpenAPI JSON": check_endpoint_found(row, "openapi_json_url"),
                "Findable Swagger": check_endpoint_found(row, "swagger_url"),
                "Findable Swagger JSON": check_endpoint_found(row, "swagger_json_url"),
            }
            # If any endpoint for this vendor passes a check, mark as True
            for k, v in checks.items():
                if v:
                    vendor_results[vendor][k] = True

    # Next, handle vendors with orgs in org_to_npi.csv but not in enriched_endpoints.csv
    for vendor in org_to_npi:
        if vendor not in vendor_results:
            # For each org_id, check the three criteria
            reachable = False
            has_onpi = False
            https_org_url = False
            for org_id, npi_value in org_to_npi[vendor]:
                # Reachable: base domain is responsive
                base_domain = get_base_domain(org_id)
                if is_domain_responsive(base_domain):
                    reachable = True
                # Has ONPI: valid 10-digit NPI
                if is_valid_npi(npi_value):
                    has_onpi = True
                # HTTPS ORG URL: starts with https://
                if is_valid_https_url(org_id):
                    https_org_url = True
            vendor_results[vendor] = {c[0]: False for c in CHECKS}
            vendor_results[vendor]["Reachable"] = reachable
            vendor_results[vendor]["Has ONPI"] = has_onpi
            vendor_results[vendor]["HTTPS ORG URL"] = https_org_url
            vendor_results[vendor]["_count"] = sum([reachable, has_onpi, https_org_url])

    # Count number of passing checks for sorting (for those with real data)
    for vendor in vendor_results:
        vendor_results[vendor]["_count"] = sum(
            vendor_results[vendor][c[0]] for c in CHECKS
        )
    return vendor_results

def write_markdown_report(vendor_results, vendor_map, output_path):
    # Ensure every vendor from vendor_map is present, add as all-fail if missing
    for vendor in set(vendor_map.values()):
        if vendor not in vendor_results:
            vendor_results[vendor] = {c[0]: False for c in CHECKS}
            vendor_results[vendor]["_count"] = 0

    # Sort vendors by number of passing checks, descending
    sorted_vendors = sorted(
        vendor_results.items(),
        key=lambda x: (-x[1]["_count"], x[0].lower())
    )

    # HTML table header
    html = []
    html.append("<table>")
    html.append("  <thead>")
    html.append("    <tr>")
    html.append("      <th>Vendor</th>")
    for c, _ in CHECKS:
        html.append(f"      <th>{c}</th>")
    html.append("    </tr>")
    html.append("  </thead>")
    html.append("  <tbody>")

    for vendor, results in sorted_vendors:
        html.append("    <tr>")
        html.append(f"      <td>{vendor}</td>")
        for c, _ in CHECKS:
            passed = results[c]
            badge_url = BADGES[passed].format(label=c.replace(' ', '%20'))
            alt_text = f"{c}: {'Pass' if passed else 'Fail'}"
            html.append(f'      <td><img src="{badge_url}" alt="{alt_text}" title="{alt_text}"></td>')
        html.append("    </tr>")
    html.append("  </tbody>")
    html.append("</table>")

    # Write to file
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("# CEHRT FHIR Vendor Compliance Dashboard\n\n")
        f.write("This dashboard lists CEHRT vendors in order of their compliance with a scrappable FHIR ecosystem. Each column represents a compliance check, and each cell shows a shield.io badge indicating pass (green) or fail (red).\n\n")
        for line in html:
            f.write(line + "\n")

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    list_sources_path = os.path.join(base_dir, "prod_data", "list_sources_summary.csv")
    enriched_path = os.path.join(base_dir, "data", "output_data", "enriched_endpoints.csv")
    output_path = os.path.join(base_dir, "CEHRT_FHIR_Report.md")

    vendor_map = load_vendor_mapping(list_sources_path)
    org_to_npi_path = os.path.join(base_dir, "data", "output_data", "normalized_csv_files", "org_to_npi.csv")
    vendor_results = aggregate_vendor_compliance(enriched_path, org_to_npi_path, vendor_map)
    write_markdown_report(vendor_results, vendor_map, output_path)
    print(f"Dashboard written to {output_path}")

if __name__ == "__main__":
    main()
