# Address Component Analysis Summary

## What This Analysis Does
This analysis examines address entries in FHIR JSON files and dynamically discovers all 
address subcomponents present in the data. It tracks the presence of address fields and 
calculates percentages for each subcomponent found (such as line, city, state, postalCode, country, etc.).

- **Discovery Method:** Dynamic scanning of all address dictionary keys
- **Components Tracked:** All subfields found in address objects (not just predefined ones)
- **Percentage Calculations:** Based on files that have address fields
- **Examples:** Longest/shortest/random by filename length

## Processing Results
**Files Processed:** 362222
**Files Failed:** 0

## Address Field Presence

| Category | Count | Percentage | Longest Example | Shortest Example | Random Example |
|----------|-------|------------|-----------------|------------------|----------------|
| Has Address | 271640 | 75.0% | [entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/abeo_solutions_inc_87133ed24a4073af176beaf74cd27a1e/organization/entry_Organization_1811435a7ea-7a1b883e-e0f0-4cb2-b938-4b6fd8d60f0c.json) | [entry_Organization_001.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/1life_healthcare_inc_b8bf6b68b0098021b1122dda499a9ab0/organization/entry_Organization_001.json) | [entry_Organization_oB3nMEqxOFVmo8B.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/advanced_data_systems_corporation_39bf6843af633b73ecc1a2a375a3e6c8/organization/entry_Organization_oB3nMEqxOFVmo8B.json) |
| No Address | 90582 | 25.0% | [entry_Endpoint_idFA6NjJ01p.WnqGN2lfXufQ.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/aarista_technology_llc_98dd118c306868b25684644c95fe4c75/endpoint/entry_Endpoint_idFA6NjJ01p.WnqGN2lfXufQ.json) | [entry_Endpoint_test.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/adaptamed_llc_47d439f4130f7692caea161fa0b4d2bd/endpoint/entry_Endpoint_test.json) | [entry_Endpoint_ChirpProduction.json](https://github.com/ftrotter-gov/npd_ehr_scrape_cache/tree/main/cache/fhir_json_cache/1life_healthcare_inc_e2a16a6a73d6e80212dc18becacfe05c/endpoint/entry_Endpoint_ChirpProduction.json) |

## Address Component Breakdown
*(Percentages are of files that have address fields)*

| Component | Count | Percentage |
|-----------|-------|------------|
| City | 263367 | 97.0% |
| Country | 126983 | 46.7% |
| District | 66 | 0.0% |
| Extension | 234 | 0.1% |
| Line | 263133 | 96.9% |
| Period | 214 | 0.1% |
| Postalcode | 263512 | 97.0% |
| State | 264270 | 97.3% |
| Text | 80080 | 29.5% |
| Type | 79699 | 29.3% |
| Use | 1390 | 0.5% |
