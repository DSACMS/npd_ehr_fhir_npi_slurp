# Telecom Email Analysis Summary

## What This Analysis Does
This analysis examines telecom entries in FHIR JSON files that have "email" as the system value. 
It validates email addresses using standard RFC-compliant regex patterns and categorizes them 
as valid or invalid. Files without any email telecoms are also tracked.

- **Validation Method:** Standard RFC-compliant email regex
- **Categories:** Valid email, invalid email, no email telecoms
- **Examples:** Longest/shortest/random by email character length

## Processing Results
**Files Processed:** 24
**Files Failed:** 0
**Files Without Email Telecoms:** 23
**Total Email Categories Found:** 1

## Email Validation Results

| Email Category | Count | Longest Example | Shortest Example | Random Example |
|----------------|-------|-----------------|------------------|----------------|
| Valid Email | 1 | [entry_Organization_id4RuGBx9NgSyfDyY1AQo1UA.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/varian_medical_systems_fe4a364477dca33cd3e28929702a7bba/organization/entry_Organization_id4RuGBx9NgSyfDyY1AQo1UA.json) | [entry_Organization_id4RuGBx9NgSyfDyY1AQo1UA.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/varian_medical_systems_fe4a364477dca33cd3e28929702a7bba/organization/entry_Organization_id4RuGBx9NgSyfDyY1AQo1UA.json) | [entry_Organization_id4RuGBx9NgSyfDyY1AQo1UA.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/blob/main/cehrt_fhir_json/varian_medical_systems_fe4a364477dca33cd3e28929702a7bba/organization/entry_Organization_id4RuGBx9NgSyfDyY1AQo1UA.json) |

**Total Email Telecoms Found:** 1
