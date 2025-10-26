# NPI Identifier Analysis Summary

## What This Analysis Does
This analysis examines identifier entries in FHIR JSON files that have 
"http://hl7.org/fhir/sid/us-npi" as the system. It validates NPI format (10 digits 
starting with '1') and tracks single vs multiple NPIs per record. Special tracking 
identifies the record with the most NPIs.

- **Validation Method:** 10 digits starting with '1' (no Luhn checksum)
- **Categories:** Single valid/invalid NPI, multiple NPIs (all/some valid)
- **Special Features:** Tracks record with most NPIs, uses NPI count for examples
- **Examples:** Longest/shortest/random by NPI list length (not character length)

## Processing Results
**Files Processed:** 23
**Files Failed:** 0
**Files Without NPI Identifiers:** 21
**Total NPI Categories Found:** 2

## NPI Analysis Results

| NPI Category | Count | Longest Example | Shortest Example | Random Example |
|--------------|-------|-----------------|------------------|----------------|
| Single Invalid NPI | 1 | [entry_Organization_5427.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/omnimd_inc_925c7fc1b4c1c165e0e0adb3c396a0b6/organization/entry_Organization_5427.json) | [entry_Organization_5427.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/omnimd_inc_925c7fc1b4c1c165e0e0adb3c396a0b6/organization/entry_Organization_5427.json) | [entry_Organization_5427.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/omnimd_inc_925c7fc1b4c1c165e0e0adb3c396a0b6/organization/entry_Organization_5427.json) |
| Single Valid NPI | 1 | [entry_Organization_5429.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/omnimd_inc_925c7fc1b4c1c165e0e0adb3c396a0b6/organization/entry_Organization_5429.json) | [entry_Organization_5429.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/omnimd_inc_925c7fc1b4c1c165e0e0adb3c396a0b6/organization/entry_Organization_5429.json) | [entry_Organization_5429.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/omnimd_inc_925c7fc1b4c1c165e0e0adb3c396a0b6/organization/entry_Organization_5429.json) |

**Total NPI Records Found:** 2

## Record with Most NPIs
**File:** [entry_Organization_5427.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/omnimd_inc_925c7fc1b4c1c165e0e0adb3c396a0b6/organization/entry_Organization_5427.json)
**NPI Count:** 1
**NPIs:** `2342342423`
