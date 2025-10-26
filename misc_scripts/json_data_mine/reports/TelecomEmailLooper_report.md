# Telecom Email Analysis Summary

## What This Analysis Does
This analysis examines telecom entries in FHIR JSON files that have "email" as the system value. 
It validates email addresses using standard RFC-compliant regex patterns and categorizes them 
as valid or invalid. Files without any email telecoms are also tracked.

- **Validation Method:** Standard RFC-compliant email regex
- **Categories:** Valid email, invalid email, no email telecoms
- **Examples:** Longest/shortest/random by email character length

## Processing Results
**Files Processed:** 362222
**Files Failed:** 0
**Files Without Email Telecoms:** 361127
**Total Email Categories Found:** 1

## Email Validation Results

| Email Category | Count | Longest Example | Shortest Example | Random Example |
|----------------|-------|-----------------|------------------|----------------|
| Valid Email | 1098 | [entry_Organization_CMD-CMZCC-Organization-2.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/curemd_com_inc_3e771a3e2122c9fa913e71a26259cca2/organization/entry_Organization_CMD-CMZCC-Organization-2.json) | [entry_Organization_idFA6NjJ01p.WnqGN2lfXufQ.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/aarista_technology_llc_98dd118c306868b25684644c95fe4c75/organization/entry_Organization_idFA6NjJ01p.WnqGN2lfXufQ.json) | [entry_Organization_CMD-CM09M-Organization-3.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/curemd_com_inc_3e771a3e2122c9fa913e71a26259cca2/organization/entry_Organization_CMD-CM09M-Organization-3.json) |

**Total Email Telecoms Found:** 1098
